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
  echo "Performs the compilation of CTA through a dedicated Kubernetes pod."
  echo "The pod persists between runs of this script (unless the --reset flag is specified), which ensures that the build does not need to happen from scratch."
  echo "It is also able to deploy the built rpms via minikube for a basic testing setup."
  echo ""
  echo "Important prerequisite: this script expects a CTA/ directory in /home/cirunner/shared/ on a VM"
  echo ""
  echo "Usage: $0 [options]"
  echo ""
  echo "options:"
  echo "  -r, --reset:                              Shut down the compilation pod and start a new one to ensure a fresh build."
  echo "      --skip-build:                        Skips the build step."
  echo "      --skip-deploy:                        Skips the redeploy step."
  echo "      --skip-cmake:                         Skips the cmake step of the build_rpm stage during the compilation process."
  echo "      --skip-unit-tests:                    Skips the unit tests. Speeds up the build time by not running the unit tests."
  echo "      --cmake-build-type <build-type>:      Specifies the build type for cmake. Must be one of [Release, Debug, RelWithDebInfo, or MinSizeRel]."
  echo "      --force-install:                      Adds the --install flag to the build_rpm step, regardless of whether the pod was reset or not."
  exit 1
}

compile_deploy() {

  # Input args
  local reset=false
  local skip_build=false
  local skip_deploy=false
  local skip_cmake=false
  local skip_unit_tests=false
  local cmake_build_type=""
  local force_install=false

  # Defaults
  local num_jobs=8
  local restarted=false
  local build_namespace="build"
  local deploy_namespace="dev"
  local src_dir="/home/cirunner/shared"
  local build_pod_name="build-pod"
  local xrootd_version="5" # TODO
  local cta_version=${xrootd_version}
  local vcs_version="dev"
  local xrootd_ssi_version="5" # TODO
  local scheduler_type="objectstore"
  local oracle_support="ON"

  # Parse command line arguments
  while [[ "$#" -gt 0 ]]; do
    case $1 in
      -r | --reset)
        reset=true
        ;;
      --skip-build)
        skip_build=true
        ;;
      --skip-deploy)
        skip_deploy=true
        ;;
      --skip-cmake)
        skip_cmake=true
        ;;
      --skip-unit-tests)
        skip_unit_tests=true
        ;;
      --force-install)
        force_install=true
        ;;
      --cmake-build-type) 
        if [[ $# -gt 1 ]]; then
          if [ "$2" != "Release" ] && [ "$2" != "Debug" ] && [ "$2" != "RelWithDebInfo" ] && [ "$2" != "MinSizeRel" ]; then
              echo "--cmake-build-type must be one of [Release, Debug, RelWithDebInfo, or MinSizeRel]."
              exit 1
          fi
          cmake_build_type="$2"
          shift
        else
          echo "Error: --cmake-build-type requires an argument"
          usage
        fi
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
  if kubectl get namespace "${build_namespace}" &> /dev/null; then
      echo "Found existing namespace ${build_namespace}."
  else
      echo "Creating namespace: ${build_namespace}"
      kubectl create namespace "${build_namespace}"
  fi

  if [ ${skip_build} = false ]; then
    # Delete old pod
    if [ ${reset} = true ]; then
      echo "Shutting down the existing compilation pod..."
      kubectl delete pod ${build_pod_name} -n ${build_namespace}
    fi

    # Create the pod if it does not exist
    if kubectl get pod ${build_pod_name} -n ${build_namespace} > /dev/null 2>&1; then
      echo "Pod ${build_pod_name} already exists"
    else
      restarted=true
      echo "Starting a new compilation pod: ${build_pod_name}..."
      # TODO: OS flag
      kubectl create -f ${src_dir}/CTA/continuousintegration/orchestration/pods/pod-build-alma9.yml -n ${build_namespace}
      kubectl wait --for=condition=ready pod/${build_pod_name} -n ${build_namespace}
      echo "Building SRPMs..."
      kubectl exec -it ${build_pod_name} -n ${build_namespace} -- ./shared/CTA/continuousintegration/ci_helpers/build_srpm.sh \
                                                                                                          --build-dir build_srpm \
                                                                                                          --cta-version ${cta_version} \
                                                                                                          --vcs-version ${vcs_version} \
                                                                                                          --xrootd-version ${xrootd_version} \
                                                                                                          --scheduler-type ${scheduler_type} \
                                                                                                          --oracle-support ${oracle_support} \
                                                                                                          --install \
                                                                                                          --jobs ${num_jobs}
    fi

    echo "Compiling the CTA project from source directory"

    local build_rpm_flags="--jobs ${num_jobs}"

    if [ ${restarted} = true ] || [ ${force_install} = true ]; then
      build_rpm_flags+=" --install"
    elif [ ${skip_cmake} = true ]; then
      # It should only be possible to skip cmake if the pod was not restarted
      build_rpm_flags+=" --skip-cmake"
    fi
    if [ ${skip_unit_tests} = true ]; then
      build_rpm_flags+=" --skip-unit-tests"
    fi
    if [[ ! ${cmake_build_type} = "" ]]; then
      build_rpm_flags+=" --cmake-build-type ${cmake_build_type}"
    fi

    echo "Building RPMs..."
    kubectl exec -it ${build_pod_name} -n ${build_namespace} -- ./shared/CTA/continuousintegration/ci_helpers/build_rpm.sh \
                                                                                                      --build-dir build_rpm \
                                                                                                      --srpm-dir build_srpm/RPM/SRPMS \
                                                                                                      --cta-version ${cta_version} \
                                                                                                      --vcs-version ${vcs_version} \
                                                                                                      --xrootd-version ${xrootd_version} \
                                                                                                      --xrootd-ssi-version ${xrootd_ssi_version} \
                                                                                                      --scheduler-type ${scheduler_type} \
                                                                                                      --oracle-support ${oracle_support} \
                                                                                                      ${build_rpm_flags}

    echo "Build successfull"
  fi

  if [ ${skip_deploy} = false ]; then
    echo "Redeploying CTA pods..."
    ./redeploy.sh -n ${deploy_namespace}
    echo "Pods redeployed"
  fi
}

compile_deploy "$@"