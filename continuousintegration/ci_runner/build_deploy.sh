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

# Help message
usage() {
  echo "Performs the compilation of CTA through a dedicated Kubernetes pod."
  echo "The pod persists between runs of this script (unless the --reset flag is specified), which ensures that the build does not need to happen from scratch."
  echo "It is also able to deploy the built rpms via minikube for a basic testing setup."
  echo ""
  echo "Important prerequisite: this script expects a CTA/ directory in /home/cirunner/shared/ on a VM"
  echo ""
  echo "Usage: $0 [-r|--reset] [-d|--redeploy] [--skip-cmake]"
  echo ""
  echo "Flags:"
  echo "  -r, --reset         Shut down the compilation pod and start a new one to ensure a fresh build."
  echo "  -d, --redeploy      Redeploy the CTA pods."
  echo "      --skip-cmake    Skips the cmake step of the build_rpm stage during the compilation process."
  exit 1
}

compile_deploy() {

  # Input args
  local reset=false
  local redeploy=false
  local skip_cmake=false

  # Defaults
  local restarted=false
  local namespace="build"
  local src_dir="/home/cirunner/shared"
  local readonly compile_pod_name="cta-compile-pod"

  # Parse command line arguments
  while [[ "$#" -gt 0 ]]; do
    case $1 in
      -r | --reset)
        reset=true
        ;;
      -d | --redeploy)
        redeploy=true
        ;;
      -s | --skip-cmake)
        skip_cmake=true
        ;;
      *) 
        usage
        ;;
    esac
    shift
  done

  # Check if src_dir specified
  echo "Checking whether CTA directory exists in \"${src_dir}\" exists..."
  if [ ! -d "${src_dir}/CTA" ]; then
    echo "Error: CTA directory not present in \"${src_dir}\"."
    exit 1;
  fi
  echo "Directory found"

  # Check if namespace exists
  if kubectl get namespace "${namespace}" &> /dev/null; then
      echo "Namespace ${namespace} exists."
  else
      # Create namespace
      echo "Creating namespace: ${namespace}"
      kubectl create namespace "${namespace}"
  fi

  if $reset; then
    echo "Shutting down the existing compilation pod..."
    kubectl delete pod ${compile_pod_name} -n ${namespace}
  fi

  # Create the pod if it does not exist
  if kubectl get pod ${compile_pod_name} -n ${namespace} > /dev/null 2>&1; then
    echo "Pod ${compile_pod_name} already exists"
  else
    echo "Starting a new compilation pod..."
    restarted=true
    kubectl create -f ${src_dir}/CTA/continuousintegration/orchestration/cta-compile-pod.yml -n ${namespace}
    kubectl wait --for=condition=ready pod/${compile_pod_name} -n ${namespace}
    echo "Building SRPMs..."
    kubectl exec -it ${compile_pod_name} -n ${namespace} -- /bin/sh -c ./shared/CTA/continuousintegration/ci_helpers/build_srpm.sh --install alma9 --jobs 4
  fi

  echo "Compiling the CTA project from source directory"

  local build_rpm_flags=" --jobs 8"

  if [ "${restarted}" = true ]; then
    build_rpm_flags+=" --install alma9"
  elif [ "${skip_cmake}" = true]; then
    # It should only be possible to skip cmake if the pod was not restarted
    build_rpm_flags+=" --skip-cmake"
  fi


  echo "Building RPMs..."
  kubectl exec -it ${compile_pod_name} -n ${namespace} -- /bin/sh -c ./shared/CTAcontinuousintegration/ci_helpers/build_rpm.sh ${build_rpm_flags}

  echo "Compilation successfull"

  if [ "${redeploy}" = "true" ]; then
    echo "Redeploying CTA pods..."
    ./redeploy.sh -n dev
    echo "Pods redeployed"
  fi
}

compile_deploy "$@"