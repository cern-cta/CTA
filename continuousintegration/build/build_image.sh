#!/bin/bash -e

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2024 CERN
# @license      This program is free software, distributed under the terms of the GNU General Public
#               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
#               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
#               option) any later version.
#
#               This program is distributed in the hope that it will be useful, but WITHOUT ANY
#               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
#               PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
#               In applying this licence, CERN does not waive the privileges and immunities
#               granted to it by virtue of its status as an Intergovernmental Organization or
#               submit itself to any jurisdiction.

set -e
source "$(dirname "${BASH_SOURCE[0]}")/../utils/log_wrapper.sh"

usage() {
  echo "Usage: $0 [options] -t|--tag <image_tag> -s|--rpm-src <rpm source>"
  echo ""
  echo "Builds an image based on the CTA rpms"
  echo "  -t, --tag <image_tag>:          Docker image tag. For example \"-t dev\""
  echo "  -s, --rpm-src <rpm source>:     Path to the RPMs to be installed. Can be absolute or relative to where the script is executed from. For example \"-s build_rpm/RPM/RPMS/x86_64\""
  echo ""
  echo "options:"
  echo "  -h, --help:                         Shows help output."
  echo "  -n, --name:                         The Docker image name. Defaults to ctageneric"
  echo "  -l, --load-into-minikube:           Takes the image from the podman registry and ensures that it is present in the image registry used by minikube."
  echo "  -c, --container-runtime <runtime>:  The container runtime to use for the build container. Defaults to podman."
  echo "      --dockerfile <path>:            Path to the Dockerfile (default: 'alma9/Dockerfile'). Should be relative to the repository root."
  echo "      --yum-repos-dir <path>:         Directory containing yum.repos.d/ on the host. Should be relative to the repository root."
  echo "      --yum-versionlock-file <path>:  Path to versionlock.list on the host. Should be relative to the repository root."
  exit 1
}

buildImage() {

  # Default values
  local rpm_src=""
  local image_tag=""
  local image_name="ctageneric"
  local container_runtime="podman"
  local rpm_default_src="image_rpms"
  local yum_repos_dir="continuousintegration/docker/alma9/etc/yum.repos.d/"
  local yum_versionlock_file="continuousintegration/docker/alma9/etc/yum/pluginconf.d/versionlock.list"
  local dockerfile="continuousintegration/docker/alma9/Dockerfile"
  local load_into_minikube=false

  while [[ "$#" -gt 0 ]]; do
    case "$1" in
    -h | --help) usage ;;
    -c | --container-runtime)
      if [[ $# -gt 1 ]]; then
        if [ "$2" != "docker" ] && [ "$2" != "podman" ]; then
          echo "-c | --container-runtime must be one of [docker, podman]."
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
    --dockerfile)
      if [[ $# -gt 1 ]]; then
        dockerfile="$2"
        shift
      else
        echo "Error: --dockerfile requires an argument"
        exit 1
      fi
      ;;
    --yum-repos-dir)
      if [[ $# -gt 1 ]]; then
        yum_repos_dir="$2"
        shift
      else
        echo "Error: --yum-repos-dir requires an argument"
        exit 1
      fi
      ;;
    --yum-versionlock-file)
      if [[ $# -gt 1 ]]; then
        yum_versionlock_file="$2"
        shift
      else
        echo "Error: --yum-versionlock-file requires an argument"
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

  if [ -z "${image_tag}" ]; then
    echo "Failure: Missing mandatory argument -t | --tag"
    usage
  fi

  if [ -z "${rpm_src}" ]; then
    echo "Failure: Missing mandatory argument -s | --rpm-src"
    usage
  fi

  # navigate to root directory
  project_root=$(git rev-parse --show-toplevel)
  cd "${project_root}"

  # Copy the rpms into a predefined rpm directory
  # This is important to ensure that the RPMs are accessible from the Docker build context
  # (as the provided location might be outside of the project root)
  trap 'rm -rf ${rpm_default_src}' EXIT
  mkdir -p ${rpm_default_src}
  cp -r ${rpm_src} ${rpm_default_src}

  echo "Building image ${image_name}:${image_tag}"
  (
    set -x
    ${container_runtime} build . -f ${dockerfile} \
      -t ${image_name}:${image_tag} \
      --network host \
      --build-arg YUM_REPOS_DIR=${yum_repos_dir} \
      --build-arg YUM_VERSIONLOCK_FILE=${yum_versionlock_file}
  )

  if [ "$load_into_minikube" == "true" ]; then
    # This step is necessary because atm the container runtime and minikube don't share the same docker runtime and local registry
    tmpfile=$(mktemp) && trap 'rm -f $tmpfile' EXIT
    ${container_runtime} save -o $tmpfile localhost/${image_name}:${image_tag} >/dev/null 2>&1
    echo "Loading new image into minikube"
    minikube image load $tmpfile --overwrite
  fi
}

buildImage "$@"
