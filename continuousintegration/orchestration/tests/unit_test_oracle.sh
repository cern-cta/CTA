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

die() { echo "$@" 1>&2 ; exit 1; }

usage() {
  echo "Spawns a pod that performs the Oracle unit tests on the catalogue."
  echo ""
  echo "Usage: $0 -n <namespace> -i <image tag> [options]"
  echo ""
  echo "options:"
  echo "  -h, --help:                     Shows help output."
  echo "  -n, --namespace <namespace>:    Specify the Kubernetes namespace."
  echo "  -r, --registry-host <host>:     Provide the Docker registry host. Defaults to \"gitlab-registry.cern.ch/cta\"."
  echo "  -i, --image-tag <tag>:          Docker image tag for the deployment."
  exit 1
}

registry_host="gitlab-registry.cern.ch/cta"

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
  case $1 in
    -h | --help) usage ;;
    -n|--namespace) 
      namespace="$2"
      shift ;;
    -r|--registry-host) 
      registry_host="$2"
      shift ;;
    -i|--image-tag) 
      image_tag="$2"
      shift ;;
    *)
      echo "Unsupported argument: $1"
      usage
      ;;
  esac
  shift
done

if [ -z "${namespace}" ]; then
  echo "Missing mandatory argument: -n | --namespace"
  usage
fi

if [ -z "${image_tag}" ]; then
  echo "Missing mandatory argument: -i | --image-tag"
  usage
fi

helm install oracle-unit-tests ../helm/oracle-unit-tests --namespace ${NAMESPACE} \
                                                         --set global.image.registry="${registry_host}" \
                                                         --set global.image.tag="${image_tag}" \
                                                         --wait --wait-for-jobs --timeout 30m
