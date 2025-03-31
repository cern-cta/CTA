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
  echo "Usage: $0 [options] -t|--tag <image_tag>"
  echo ""
  echo "Builds an image based on the CTA rpms"
  echo "  -t, --tag <image_tag>:          Docker image tag. For example \"-t dev\""
  echo ""
  echo "options:"
  echo "  -h, --help:                         Shows help output."
  echo "  -n, --name:                         The Docker image name. Defaults to cta-rest-api"
  echo "  -l, --load-into-minikube:           Takes the image from the podman registry and ensures that it is present in the image registry used by minikube."
  echo "      --dockerfile <path>:            Path to the Dockerfile (default: 'alma9/Dockerfile'). Should be relative to the repository root."
  exit 1
}

buildImage() {

  # Default values
  local image_tag=""
  local image_name="cta-rest-api"
  local dockerfile="continuousintegration/docker/alma9/Dockerfile"
  local load_into_minikube=false

  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      -h | --help) usage ;;
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
      *)
        echo "Unsupported argument: $1"
        usage ;;
    esac
    shift
  done

  if [ -z "${image_tag}" ]; then
    echo "Failure: Missing mandatory argument -t | --tag"
    usage
  fi

  # navigate to root directory
  cd "$(dirname "$0")"
  cd ../../

  # Navigate to API dir
  cd api
  echo "Building image ${image_name}:${image_tag}"
  (set -x; podman build . -f ${dockerfile} \
                          -t ${image_name}:${image_tag})

  if [ "$load_into_minikube" == "true" ]; then
    # This step is necessary because atm podman and minikube don't share the same docker runtime and local registry
    tmpfile=$(mktemp) && trap 'rm -f $tmpfile' EXIT
    podman save -o $tmpfile localhost/${image_name}:${image_tag} > /dev/null 2>&1
    echo "Loading new image into minikube"
    minikube image load $tmpfile --overwrite
  fi
}

buildImage "$@"
