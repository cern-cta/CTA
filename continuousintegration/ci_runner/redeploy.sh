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
  echo "Will (re)deploy the local minikube instance with the latest rpms."
  echo "These rpms are assumed to be located in CTA/build_rpm, as done by the build_rpm.sh script in ci_helpers."
  echo ""
  echo "Usage: $0 [-n|--namespace <namespace>] [-t|--tag <tag>] [-s|--rpm-src <rpm source>]"
  echo ""
  echo "Flags:"
  echo "  -n, --namespace <namespace>:  Specify the Kubernetes namespace. Defaults to \"dev\" if not provided."
  echo "  -t, --tag <tag>:              Image tag to use. Defaults to \"dev\" if not provided."
  echo "  -s, --rpm-src <rpm source>:   Path to the RPMs that should be deployed. Defaults to CTA/build_rpm/RPM/RPMS/x86_64 if not provided."
  exit 1
}

redeploy() {
  # Default values
  local kube_namespace="dev"
  local image_tag="dev"
  local rpm_src="build_rpm/RPM/RPMS/x86_64"

  # Parse command line arguments
  while [[ "$#" -gt 0 ]]; do
    case $1 in
      -n | --namespace)
        if [[ $# -gt 1 ]]; then
          kube_namespace="$2"
          shift
        else
          echo "Error: -n|--namespace requires an argument"
          exit 1
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
      *) 
        usage
        ;;
    esac
    shift
  done

  if [ "$#" -ge 3 ]; then
    usage
    exit 1
  fi

  # Script should be run as cirunner
  if [[ $(whoami) != 'cirunner' ]]; then
    echo "Current user is $(whoami), aborting. Script must run as cirunner"
    exit 1
  fi

  ###################################################################################################

  # navigate to root directory
  cd "$(dirname "$0")"
  cd ../../

  # Delete previous instance, if it exists
  if ! kubectl get namespace "$NAMESPACE" &> /dev/null; then
    echo "Deleting the old namespace"
    ./continuousintegration/orchestration/delete_instance.sh -n ${kube_namespace}
  fi

  # Clear the old image and namespace
  if podman inspect ctageneric:${image_tag} &> /dev/null; then
    echo "Deleting old image and removing it from minikube"
    podman rmi ctageneric:${image_tag}
    minikube image rm localhost/ctageneric:${image_tag}
  fi

  ## Create and load the new image
  echo "Building image based on ${rpm_src}"
  ./continuousintegration/ci_runner/prepareImage.sh --tag ${image_tag} --rpm-src ${rpm_src}
  echo "Loading new image into minikube"
  minikube image load  localhost/ctageneric:${image_tag}

  # Redeploy containers
  echo "Redeploying container"
  cd continuousintegration/orchestration
  ./create_instance.sh -n $kube_namespace -i ${image_tag} -D -O -d internal_postgres.yaml
}

redeploy "$@"