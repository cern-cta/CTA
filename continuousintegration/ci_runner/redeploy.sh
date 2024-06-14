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
  echo "Usage: $0 [-n <namespace>] [-t <tag>] [-d <root directory>]"
  echo ""
  echo "Flags:"
  echo "  -n, --namespace <namespace>: Specify the Kubernetes namespace (optional)"
  echo "  -t, --tag <tag>: Specify a tag (optional)"
  echo "  -d, --root-dir <root directory>: Specify the root directory where the CTA directory is located (optional)"
  exit 1
}

# Default values
KUBE_NAMESPACE="stress"
IMAGE_TAG="dev"
ROOT_DIR="~"

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
  case $1 in
    -n | --namespace)
      KUBE_NAMESPACE="$2"
      shift
      ;;
    -t | --tag)
      IMAGE_TAG="$2"
      shift
      ;;
    -d | --root-dir)
      ROOT_DIR="$2"
      shift
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
if [[ $(whoami) != 'cirunner' ]]
then
    echo "Current user is $(whoami), aborting. Script must run as cirunner"
    exit 1
fi

# Delete previous instance, if it exists
if ! kubectl get namespace "$NAMESPACE" &> /dev/null; then
  echo "Deleting the old namespace"
  cd $ROOT_DIR/CTA/continuousintegration/orchestration/
  ./delete_instance.sh -n $KUBE_NAMESPACE
fi

# Clear the old image and namespace
if podman inspect ctageneric:$IMAGE_TAG &> /dev/null; then
  echo "Deleting old image and removing it from minikube"
  podman rmi ctageneric:$IMAGE_TAG
  minikube image rm localhost/ctageneric:$IMAGE_TAG
fi

echo "Cleaning up ctageneric.tar..."
cd $ROOT_DIR/CTA/continuousintegration/ci_runner
rm -rf ctageneric.tar

###################################################################################################


## Create and load the new images
# Prepare new image
echo "Preparing new image"
cd $ROOT_DIR/CTA/continuousintegration/ci_runner # should already be here
./prepareImage.sh -s $ROOT_DIR/CTA_rpm/RPM/RPMS/x86_64 -t $IMAGE_TAG -d $ROOT_DIR
# Save the image in a tar file
echo "Saving new image"
podman save -o ctageneric.tar localhost/ctageneric:$IMAGE_TAG
# Load the new image
echo "Loading new image into minikube"
minikube image load ctageneric.tar localhost/ctageneric:$IMAGE_TAG

# Redeploy containers
echo "Redeploying container"
cd $ROOT_DIR/CTA/continuousintegration/orchestration
./create_instance.sh -n $KUBE_NAMESPACE -i $IMAGE_TAG -D -O -d internal_postgres.yaml
