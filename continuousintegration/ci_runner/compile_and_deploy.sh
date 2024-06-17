#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2022 CERN
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

# Help message
usage() {
  echo "Performs the compilation of CTA through a dedicated Kubernetes pod."
  echo "The pod persists between runs of this script (unless the --reset flag is specified), which ensures that the build does not need to happen from scratch."
  echo "It is also able to deploy the built rpms via minikube for a basic testing setup."
  echo ""
  echo "Important prerequisite: this script expects a CTA/ directory in /home/cirunner/shared/ on a VM"
  echo ""
  echo "Usage: $0 [-r] [-d]"
  echo ""
  echo "Flags:"
  echo "  -r, --reset         Shut down the compilation pod and start a new one to ensure a fresh build."
  echo "  -c, --cmake         Enforces the execution of the cmake step. If not provided, cmake is only executed once when the pod is created."
  echo "  -d, --redeploy      Redeploy the CTA pods."
  exit 1
}

# Default values
RESET=false
REDEPLOY=false
RESTARTED=false
NAMESPACE="build"
EXEC_CMAKE=false
SRC_DIR="/home/cirunner/shared"

POD_NAME="cta-compile-pod"

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
  case $1 in
    -r  |--reset) RESET=true ;;
    -d  |--redeploy) REDEPLOY=true ;;
    -c  |--cmake) EXEC_CMAKE=true ;;
    *)
    usage ;;
  esac
  shift
done

# Check if SRC_DIR specified
echo "Checking whether CTA directory exists in \"$SRC_DIR\" exists..."
if [ ! -d "$SRC_DIR/CTA" ]; then
  echo "Error: CTA directory not present in \"$SRC_DIR\"."
  exit -1
fi
echo "Directory found"

# Check if namespace exists
if kubectl get namespace "$NAMESPACE" &> /dev/null; then
    echo "Namespace $NAMESPACE exists."
else
    # Create namespace
    kubectl create namespace "$NAMESPACE"
fi

if $RESET; then
  echo "Shutting down the existing compilation pod..."
  kubectl delete pod $POD_NAME -n $NAMESPACE
fi

# Create the pod if it does not exist
if kubectl get pod $POD_NAME -n $NAMESPACE > /dev/null 2>&1; then
  echo "Pod $POD_NAME already exists"
else
  echo "Starting a new compilation pod..."
  RESTARTED=true
  kubectl create -f $SRC_DIR/CTA/continuousintegration/orchestration/cta-compile-pod.yml -n $NAMESPACE
  kubectl wait --for=condition=ready pod/$POD_NAME -n $NAMESPACE
  echo "Installing packages into container"
  kubectl exec -it $POD_NAME -n $NAMESPACE -- /bin/sh -c ' \
    cd /shared/CTA && \
    cp -f continuousintegration/docker/ctafrontend/alma9/repos/*.repo /etc/yum.repos.d/ && \
    cp -f continuousintegration/docker/ctafrontend/alma9/yum/pluginconf.d/versionlock.list /etc/yum/pluginconf.d/ && \
    yum -y install epel-release almalinux-release-devel git && \
    yum -y install git wget gcc gcc-c++ cmake3 make rpm-build yum-utils && \
    ./continuousintegration/docker/ctafrontend/alma9/installOracle21.sh'
  echo "Basic pod created"
  echo "Building SRPMs..."
  kubectl exec -it $POD_NAME -n $NAMESPACE -- /bin/sh -c ' \
    cd /tmp && \
    mkdir CTA_srpm && \
    cd CTA_srpm && \
    cmake3 -DPackageOnly:Bool=true /shared/CTA && \
    make cta_srpm -j 4 && \
    yum-builddep --nogpgcheck -y /tmp/CTA_srpm/RPM/SRPMS/*'
fi

echo "Compiling the CTA project from source directory"

# TODO: add explicit cmake run flag
if [ "$RESTARTED" = true ] || [ "$EXEC_CMAKE" = true ]; then
  echo "Running cmake..."
  kubectl exec -it $POD_NAME -n $NAMESPACE -- /bin/sh -c '
    cd /shared/ && \
    mkdir -p CTA_rpm && \
    cd CTA_rpm && \
    cmake3 ../CTA'
fi

echo "Running make..."

kubectl exec -it $POD_NAME -n $NAMESPACE -- /bin/sh -c '
  cd /shared/CTA_rpm && \
  make cta_rpm  -j 4'

echo "Compilation successfull"

if [ "$REDEPLOY" = "true" ]; then
  echo "Redeploying CTA pods..."
  ./redeploy.sh --root-dir $SRC_DIR -n dev
  echo "Pods redeployed"
fi
