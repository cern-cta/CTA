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
  echo "Performs the build of CTA through a dedicated build container."
  echo "The container persists between runs of this script (unless the --reset flag is specified), which ensures that the build does not need to happen from scratch."
  echo "It is also able to deploy the built rpms via minikube for a basic testing setup."
  echo ""
  echo "Important prerequisite: this script expects a CTA/ directory in /home/cirunner/shared/ on a VM"
  echo ""
  echo "Usage: $0 [options]"
  echo ""
  echo "options:"
  echo "  -h, --help:                               Shows help output."
  echo "  -r, --reset:                              Shut down the build container and start a new one to ensure a fresh build."
  echo "  -o, --operating-system <os>:              Specifies for which operating system to build the rpms. Supported operating systems: [cc7, alma9]. Defaults to alma9 if not provided."
  echo "      --build-generator <generator>:        Specifies the build generator for cmake. Supported: [\"Unix Makefiles\", \"Ninja\"]."
  echo "      --clean-build-dir:                    Empties the RPM build directory (build_rpm/ by default), ensuring a fresh build from scratch."
  echo "      --clean-build-dirs:                   Empties both the SRPM and RPM build directories (build_srpm/ and build_rpm/ by default), ensuring a fresh build from scratch."
  echo "      --cmake-build-type <build-type>:      Specifies the build type for cmake. Must be one of [Release, Debug, RelWithDebInfo, or MinSizeRel]."
  echo "      --disable-oracle-support:             Disables support for oracle."
  echo "      --disable-ccache:                     Disables ccache for the building of the rpms."
  echo "      --skip-build:                         Skips the build step."
  echo "      --skip-deploy:                        Skips the redeploy step."
  echo "      --skip-cmake:                         Skips the cmake step of the build_rpm stage during the build process."
  echo "      --skip-debug-packages                 Skips the building of the debug RPM packages."
  echo "      --skip-unit-tests:                    Skips the unit tests. Speeds up the build time by not running the unit tests."
  echo "      --scheduler-type <scheduler-type>:    The scheduler type. Ex: objectstore."
  echo "      --force-install:                      Adds the --install flag to the build_rpm step, regardless of whether the pod was reset or not."
  exit 1
}

compile_deploy() {

  # Input args
  local clean_build_dir=false
  local clean_build_dirs=false
  local force_install=false
  local reset=false
  local skip_build=false
  local skip_deploy=false
  local skip_cmake=false
  local skip_unit_tests=false
  local skip_debug_packages=false
  local build_generator="Ninja"
  local cmake_build_type=""
  local operating_system="alma9"
  local scheduler_type="objectstore"
  local oracle_support="ON"
  local enable_ccache=true

  # Defaults
  local num_jobs=8
  local restarted=false
  local build_namespace="build"
  local deploy_namespace="dev"
  local src_dir="/home/cirunner/shared"
  local build_pod_name="cta-build"
  local cta_version="5"
  # These versions don't affect anything functionality wise
  local vcs_version="dev"
  local xrootd_ssi_version="dev"

  # Parse command line arguments
  while [[ "$#" -gt 0 ]]; do
    case $1 in
      -h | --help) usage ;;
      -r | --reset) reset=true ;;
      --clean-build-dir) clean_build_dir=true ;;
      --clean-build-dirs) clean_build_dirs=true ;;
      --disable-oracle-support) oracle_support="OFF" ;;
      --disable-ccache) enable_ccache=false ;;
      --skip-build) skip_build=true ;;
      --skip-deploy) skip_deploy=true ;;
      --skip-cmake) skip_cmake=true ;;
      --skip-unit-tests) skip_unit_tests=true ;;
      --skip-debug-packages) skip_debug_packages=true ;;
      --force-install) force_install=true ;;
      --build-generator) 
        if [[ $# -gt 1 ]]; then
          build_generator="$2"
          shift
        else
          echo "Error: --build-generator requires an argument"
          usage
        fi
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
      --scheduler-type)
        if [[ $# -gt 1 ]]; then
          scheduler_type="$2"
          shift
        else
          echo "Error: --scheduler-type requires an argument"
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
  echo "Checking whether CTA directory exists in \"${src_dir}\"..."
  if [ ! -d "${src_dir}/CTA" ]; then
    echo "Error: CTA directory not present in \"${src_dir}\"."
    exit 1
  fi
  echo "CTA directory found"

  # Check if namespace exists
  if kubectl get namespace "${build_namespace}" &>/dev/null; then
    echo "Found existing namespace ${build_namespace}."
  else
    echo "Creating namespace: ${build_namespace}"
    kubectl create namespace "${build_namespace}"
  fi

  if [ ${skip_build} = false ]; then
    # Delete old pod
    if [ ${reset} = true ]; then
      echo "Attempting shutdown of existing build pod..."
      kubectl delete pod ${build_pod_name} -n ${build_namespace} --ignore-not-found
    fi

    # Create the pod if it does not exist
    if kubectl get pod ${build_pod_name} -n ${build_namespace} >/dev/null 2>&1; then
      echo "Pod ${build_pod_name} already exists"
    else
      restarted=true
      echo "Starting a new build pod: ${build_pod_name}..."
      case "${operating_system}" in
        cc7)
          kubectl create -f ${src_dir}/CTA/continuousintegration/orchestration/pods/pod-build-cc7.yml -n ${build_namespace}
          ;;
        alma9)
          kubectl create -f ${src_dir}/CTA/continuousintegration/orchestration/pods/pod-build-alma9.yml -n ${build_namespace}
          ;;
        *)
          echo "Invalid operating system provided: ${operating_system}"
          exit 1
          ;;
      esac
      kubectl wait --for=condition=ready pod/${build_pod_name} -n ${build_namespace}
      echo "Building SRPMs..."
      local build_srpm_flags=""
      if [[ ${clean_build_dirs} = true ]]; then
        build_srpm_flags+=" --clean-build-dir"
      fi

      kubectl exec -it ${build_pod_name} -n ${build_namespace} -- ./shared/CTA/continuousintegration/ci_helpers/build_srpm.sh \
        --build-dir /shared/CTA/build_srpm \
        --build-generator "${build_generator}" \
        --create-build-dir \
        --cta-version ${cta_version} \
        --vcs-version ${vcs_version} \
        --scheduler-type ${scheduler_type} \
        --oracle-support ${oracle_support} \
        --install \
        --jobs ${num_jobs} \
        ${build_srpm_flags}
    fi

    echo "Compiling the CTA project from source directory"

    local build_rpm_flags="--jobs ${num_jobs}"

    if [ ${restarted} = true ] || [ ${force_install} = true ]; then
      # Only install if it is the first time running this or if the install is forced
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

    if [[ ${clean_build_dir} = true || ${clean_build_dirs} = true ]]; then
      build_rpm_flags+=" --clean-build-dir"
    fi

    if [[ ${skip_debug_packages} = true ]]; then
      build_rpm_flags+=" --skip-debug-packages"
    fi

    if [[ ${enable_ccache} = true ]]; then
      build_rpm_flags+=" --enable-ccache"
    fi

    echo "Building RPMs..."
    kubectl exec -it ${build_pod_name} -n ${build_namespace} -- ./shared/CTA/continuousintegration/ci_helpers/build_rpm.sh \
      --build-dir /shared/CTA/build_rpm \
      --build-generator "${build_generator}" \
      --create-build-dir \
      --srpm-dir /shared/CTA/build_srpm/RPM/SRPMS \
      --cta-version ${cta_version} \
      --vcs-version ${vcs_version} \
      --xrootd-ssi-version ${xrootd_ssi_version} \
      --scheduler-type ${scheduler_type} \
      --oracle-support ${oracle_support} \
      ${build_rpm_flags}

    echo "Build successfull"
  fi

  if [ ${skip_deploy} = false ]; then
    echo "Redeploying CTA pods..."
    bash ${src_dir}/CTA/continuousintegration/ci_runner/redeploy.sh \
      -n ${deploy_namespace} \
      --operating-system "${operating_system}" \
      --rpm-src build_rpm/RPM/RPMS/x86_64
  fi
}

compile_deploy "$@"
