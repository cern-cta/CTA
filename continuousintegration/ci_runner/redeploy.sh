#!/bin/bash

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

# Help message
usage() {
  echo "Usage: $0 [options] -s|--rpm-src <rpm source>"
  echo ""
  echo "Will (re)deploy the local minikube instance with the latest rpms."
  echo "These rpms are assumed to be located in CTA/build_rpm, as done by the build_rpm.sh script in ci_helpers."
  echo "  -s, --rpm-src <rpm source>:   Path to the RPMs to be installed. Can be absolute or relative to where the script is executed from. For example \"-s build_rpm/RPM/RPMS/x86_64\""
  echo ""
  echo "options:"
  echo "  -h, --help:                   Shows help output."
  echo "  -n, --namespace <namespace>:  Specify the Kubernetes namespace. Defaults to \"dev\" if not provided."
  echo "  -o, --operating-system <os>:  Specifies for which operating system to build the rpms. Supported operating systems: [cc7, alma9]. Defaults to alma9 if not provided."
  echo "  -t, --tag <tag>:              Image tag to use. Defaults to \"dev\" if not provided."
  echo "  -s, --rpm-src <rpm source>:   Path to the RPMs that should be deployed. Defaults to CTA/build_rpm/RPM/RPMS/x86_64 if not provided."
  exit 1
}

redeploy() {
  # Default values
  local kube_namespace="dev"
  local image_tag="dev"
  local operating_system="alma9"
  local rpm_src=""

  # Parse command line arguments
  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      -h | --help) usage ;;
      -n | --namespace)
        if [[ $# -gt 1 ]]; then
          kube_namespace="$2"
          shift
        else
          echo "Error: -n|--namespace requires an argument"
          exit 1
        fi
        ;;
      -o | --operating-system)
        if [[ $# -gt 1 ]]; then
          if [ "$2" != "cc7" ] && [ "$2" != "alma9" ]; then
            echo "-o | --operating-system must be one of [cc7, alma9]."
            exit 1
          fi
          operating_system="$2"
          shift
        else
          echo "Error: -o | --operating-system requires an argument"
          usage
        fi
        ;;
      -s | --rpm-src)
        if [[ $# -gt 1 ]]; then
          rpm_src="$2"
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
      *) usage ;;
    esac
    shift
  done

  if [ -z "${rpm_src}" ]; then
    echo "Failure: Missing mandatory argument -s | --rpm-src"
    usage
  fi

  # Script should be run as cirunner
  if [[ $(whoami) != 'cirunner' ]]; then
    # At some point this should be improved; this script shouldn't care as long as minikube is running
    echo "Current user is $(whoami), aborting. Script must run as cirunner"
    exit 1
  fi

  ###################################################################################################

  # navigate to root directory
  cd "$(dirname "$0")"
  cd ../../

  # Delete previous instance, if it exists
  if kubectl get namespace ${kube_namespace} &>/dev/null; then
    echo "Found existing namespace \"${kube_namespace}\". Deleting..."
    ./continuousintegration/orchestration/delete_instance.sh -n ${kube_namespace}
  fi

  # Clear the old image and namespace
  if podman inspect ctageneric:${image_tag} &>/dev/null; then
    echo "Deleting old image and removing it from minikube"
    podman rmi ctageneric:${image_tag}
    minikube image rm localhost/ctageneric:${image_tag}
  fi

  ## Create and load the new image
  echo "Building image based on ${rpm_src}"
  ./continuousintegration/ci_runner/prepare_image.sh --tag ${image_tag} --rpm-src "${rpm_src}" --operating-system "${operating_system}"
  # This step is necessary because atm podman and minikube don't share the same docker runtime and local registry
  # Note that this will only work when running this script on an alma9 machine. At some point we should look into abstracting this more
  podman save -o ctageneric_${image_tag}.tar localhost/ctageneric:${image_tag}
  echo "Loading new image into minikube"
  minikube image load ctageneric_${image_tag}.tar
  rm ctageneric_${image_tag}.tar

  # Redeploy containers
  echo "Redeploying containers"
  cd continuousintegration/orchestration
  ./create_instance.sh -n ${kube_namespace} -i ${image_tag} -D -O -d internal_postgres.yaml

  echo "Pods redeployed."
}

redeploy "$@"
