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
  echo "  -l, --load-into-minikube:           Takes the image from the podman registry and ensures that it is present in the image registry used by minikube."
  echo "  -c, --container-runtime <runtime>:  The container runtime to use for the build container. Defaults to podman."
  echo "      --dockerfile <path>:            Path to the Dockerfile (default: 'continuousintegration/docker/{defaultplatform}/dev-local-rpms.Dockerfile'). Should be relative to the repository root."
  echo "      --use-internal-repos:           Use the internal yum repos instead of the public repos."
  echo
  exit 1
}

buildImage() {
  project_root=$(git rev-parse --show-toplevel)

  # Default values
  local rpm_src=""
  local image_tag=""
  local image_name="ctageneric"
  local container_runtime="podman"
  local rpm_default_src="image_rpms"
  local defaultPlatform=$(jq -r .dev.defaultPlatform "${project_root}/project.json")
  local dockerfile="continuousintegration/docker/${defaultPlatform}/dev-local-rpms.Dockerfile"
  local load_into_minikube=false
  # Note that the capitalization here is intentional as this is passed directly as a build arg
  local use_internal_repos="FALSE"
  local rpm_version=""

  while [[ "$#" -gt 0 ]]; do
    case "$1" in
    -h | --help) usage ;;
    -c | --container-runtime)
      if [[ $# -gt 1 ]]; then
        if [[ "$2" != "docker" ]] && [[ "$2" != "podman" ]]; then
          echo "-c | --container-runtime is \"$2\" but must be one of [docker, podman]."
          exit 1
        fi
        container_runtime="$2"
        shift
      else
        echo "Error: -c | --container-runtime requires an argument"
        usage
      fi
      ;;
    -s | --rpm-src)
      if [[ $# -gt 1 ]]; then
        rpm_src=$(realpath "$2")
        shift
      else
        echo "Error: -s|--rpm-src requires an argument"
        exit 1
      fi
      ;;
    --rpm-version)
      if [[ $# -gt 1 ]]; then
        rpm_version="$2"
        shift
      else
        echo "Error: --rpm-version requires an argument"
        exit 1
      fi
      ;;
    -t | --tag)
      if [[ $# -gt 1 ]]; then
        image_tag="$2"
        shift
      else
        echo "Error: -t|--tag requires an argument"
        exit 1
      fi
      ;;
    -n | --name)
      if [[ $# -gt 1 ]]; then
        image_name="$2"
        shift
      else
        echo "Error: -n | --name requires an argument"
        exit 1
      fi
      ;;
    -l | --load-into-minikube)
      load_into_minikube=true
      ;;
    --use-internal-repos)
      use_internal_repos="TRUE"
      ;;
    --dockerfile)
      if [[ $# -gt 1 ]]; then
        dockerfile="$2"
        shift
      else
        echo "Error: --dockerfile requires an argument"
        exit 1
      fi
      ;;
    *)
      echo "Unsupported argument: $1"
      usage
      ;;
    esac
    shift
  done

  if [[ -z "${image_tag}" ]]; then
    echo "Failure: Missing mandatory argument -t | --tag"
    usage
  fi

  if [[ -z "${rpm_src}" ]]; then
    echo "Failure: Missing mandatory argument -s | --rpm-src"
    usage
  fi

  if [[ -z "${rpm_version}" ]]; then
    echo "Failure: Missing mandatory argument --rpm-version"
    usage
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

  echo "Building image ${image_name}:${image_tag}"
  (
    set -x
    ${container_runtime} build . -f ${dockerfile} \
      -t ${image_name}:${image_tag} \
      --network host \
      --build-arg USE_INTERNAL_REPOS=${use_internal_repos}
  )
  # Clean up again
  rm -rf "${rpm_default_src}"

  if [[ "$load_into_minikube" == "true" ]]; then
    # This step is necessary because atm the container runtime and minikube don't share the same docker runtime and local registry
    tmpfile=$(mktemp) && add_trap 'rm -f $tmpfile' EXIT
    ${container_runtime} save -o $tmpfile localhost/${image_name}:${image_tag} >/dev/null 2>&1
    echo "Loading new image into minikube"
    minikube image load $tmpfile --overwrite
  fi
}

buildImage "$@"
