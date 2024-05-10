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

IMAGE_TAG=${1:-dev} #default tag is dev
KUBE_NAMESPACE=${2:-stress}

if [ "$#" -ge 3 ]; then
    echo "Usage: The script $0 takes two optional arguments: image tag (1) and namespace name (2)"
    exit 1
fi

echo "Launching redeploy script; image tag is $IMAGE_TAG, namespace name is $KUBE_NAMESPACE"

# Script should be run as cirunner
if [[ $(whoami) != 'cirunner' ]]
then
    echo "Current user is $(whoami), aborting. Script must run as cirunner"
    exit 1
fi

# Delete previous instance, if it exists
echo "Deleting the old namespace, if it exists"
cd ~/CTA/continuousintegration/orchestration/ && ./delete_instance.sh -n $KUBE_NAMESPACE

# Clear the old image and namespace
echo "Deleting old image and removing it from minikube"
podman rmi ctageneric:$IMAGE_TAG
minikube image rm localhost/ctageneric:$IMAGE_TAG
cd ~/CTA/continuousintegration/ci_runner
rm -rf ctageneric.tar

## Create and load the new images
# Prepare new image
echo "Preparing new image"
cd ~/CTA/continuousintegration/ci_runner # should already be here
./prepareImage.sh ~/CTA_rpm/RPM/RPMS/x86_64 $IMAGE_TAG
# Save the image in a tar file
echo "Saving new image"
podman save -o ctageneric.tar localhost/ctageneric:$IMAGE_TAG
# Load the new image
echo "Loading new image into minikube"
minikube image load ctageneric.tar localhost/ctageneric:$IMAGE_TAG

# Redeploy containers
echo "Redeploying container"
cd ~/CTA/continuousintegration/orchestration
./create_instance.sh -n $KUBE_NAMESPACE -i $IMAGE_TAG -D -O -d internal_postgres.yaml


