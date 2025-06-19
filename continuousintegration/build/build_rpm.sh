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

usage() {
  echo "Usage: $0 [options] --build-dir <build-dir> --srpm-dir <srpm-directory> --scheduler-type <scheduler-type> --cta-version <cta-version> --vcs-version <vcs-version> --xrootd-ssi-version <xrootd-ssi-version>"
  echo ""
  echo "Builds the rpms."
  echo "  --build-dir <build-directory>:                Sets the build directory for the RPMs. Can be absolute or relative to where the script is being executed from. Ex: build_rpm"
  echo "  --build-generator <generator>:                Specifies the build generator for cmake. Ex: [\"Unix Makefiles\", \"Ninja\"]."
  echo "  --srpm-dir <srpm-directory>:                  The directory where the source rpms are located. Can be absolute or relative to where the script is being executed from."
  echo "  --scheduler-type <type>:                      The scheduler type. Must be one of [objectstore, pgsched]."
  echo "  --cta-version <cta-version>:                  Sets the CTA_VERSION."
  echo "  --vcs-version <vcs-version>:                  Sets the VCS_VERSION variable in cmake."
  echo "  --xrootd-ssi-version <xrootd-ssi-version>:    Sets the XROOTD_SSI_PROTOBUF_INTERFACE_VERSION variable in cmake."
  echo "  --cmake-build-type <type>:                    Specifies the build type for cmake. Must be one of [Release, Debug, RelWithDebInfo, or MinSizeRel]."
  echo "  --platform <platform>:                        Which platform the build is running for."
  echo ""
  echo "options:"
  echo "      --install-srpms:                          Installs only the SRPMS."
  echo "  -j, --jobs <num_jobs>:                        How many jobs to use for make."
  echo "      --clean-build-dir:                        Empties the build directory, ensuring a fresh build from scratch."
  echo "      --create-build-dir                        Creates the build directory if it does not exist."
  echo "      --enable-ccache:                          Enables ccache."
  echo "      --skip-cmake                              Skips the cmake step. Can be used if this script is executed multiple times in succession."
  echo "      --skip-debug-packages                     Skips the building of the debug RPM packages."
  echo "      --skip-unit-tests                         Skips the unit tests. Speeds up the build time by not running the unit tests."
  echo "      --oracle-support <ON/OFF>:                When set to OFF, will disable Oracle support. Oracle support is enabled by default."
  echo "      --use-internal-repos                      Use the internal yum repos instead of the public yum repos."
  exit 1
}

build_rpm() {
  project_root="$(realpath "$(dirname "$0")/../..")"
  # Default values for arguments
  local build_dir=""
  local build_generator=""
  local cta_version=""
  local scheduler_type=""
  local srpm_dir=""
  local vcs_version=""
  local xrootd_ssi_version=""
  local platform=""

  local enable_ccache=false
  local install_srpms=false
  local num_jobs=$(nproc --ignore=2)
  local cmake_build_type=""
  local clean_build_dir=false
  local create_build_dir=false
  local skip_cmake=false
  local skip_unit_tests=false
  local skip_debug_packages=false
  local oracle_support=true
  local use_internal_repos=false

  # Parse command line arguments
  while [[ "$#" -gt 0 ]]; do
    case "$1" in
    --build-dir)
      if [[ $# -gt 1 ]]; then
        # Convert build_dir to an absolute path to prevent possible future bugs with navigating away from the root directory
        build_dir=$(realpath "$2")
        shift
      else
        echo "Error: --build-dir requires an argument"
        usage
      fi
      ;;
    --platform)
      if [[ $# -gt 1 ]]; then
        if [ $(jq '.platforms | has("el9")' ${project_root}/project.json) != "true" ]; then
          echo "Error: platform $2 not supported. Please check the project.json for supported platforms."
        fi
        platform="$2"
        shift
      else
        echo "Error: --platform requires an argument"
        usage
      fi
      ;;
    --build-generator)
      if [[ $# -gt 1 ]]; then
        if [ "$2" != "Ninja" ] && [ "$2" != "Unix Makefiles" ]; then
          echo "Warning: build generator $2 is not officially supported. Compilation might not be successful."
        fi
        build_generator="$2"
        shift
      else
        echo "Error: --build-generator requires an argument"
        usage
      fi
      ;;
    --clean-build-dir) clean_build_dir=true ;;
    --create-build-dir) create_build_dir=true ;;
    --enable-ccache) enable_ccache=true ;;
    --cta-version)
      if [[ $# -gt 1 ]]; then
        cta_version="$2"
        shift
      else
        echo "Error: --cta-version requires an argument"
        usage
      fi
      ;;
    --scheduler-type)
      if [[ $# -gt 1 ]]; then
        if [ "$2" != "objectstore" ] && [ "$2" != "pgsched" ]; then
          echo "Error: scheduler type $2 is not one of [objectstore, pgsched]."
          exit 1
        fi
        scheduler_type="$2"
        shift
      else
        echo "Error: --scheduler-type requires an argument"
        usage
      fi
      ;;
    --srpm-dir)
      if [[ $# -gt 1 ]]; then
        # Convert srpm_dir to an absolute path to prevent possible future bugs with navigating away from the root directory
        srpm_dir=$(realpath "$2")
        shift
      else
        echo "Error: --srpm-dir requires an argument"
        usage
      fi
      ;;
    --vcs-version)
      if [[ $# -gt 1 ]]; then
        vcs_version="$2"
        shift
      else
        echo "Error: --vcs-version requires an argument"
        usage
      fi
      ;;
    --xrootd-ssi-version)
      if [[ $# -gt 1 ]]; then
        xrootd_ssi_version="$2"
        shift
      else
        echo "Error: --xrootd-ssi-version requires an argument"
        usage
      fi
      ;;
    --install-srpms) install_srpms=true ;;
    --use-internal-repos) use_internal_repos=true ;;
    -j | --jobs)
      if [[ $# -gt 1 ]]; then
        num_jobs="$2"
        shift
      else
        echo "Error: -j|--jobs requires an argument"
        usage
      fi
      ;;
    --skip-cmake) skip_cmake=true ;;
    --skip-debug-packages) skip_debug_packages=true ;;
    --skip-unit-tests) skip_unit_tests=true ;;
    --oracle-support)
      if [[ $# -gt 1 ]]; then
        if [ "$2" = "FALSE" ]; then
          oracle_support=false
        fi
        shift
      else
        echo "Error: -j|--jobs requires an argument"
        usage
      fi
      ;;
    --cmake-build-type)
      if [[ $# -gt 1 ]]; then
        if [ "$2" != "Release" ] && [ "$2" != "Debug" ] && [ "$2" != "RelWithDebInfo" ] && [ "$2" != "MinSizeRel" ]; then
          echo "--cmake-build-type is \"$2\" but must be one of [Release, Debug, RelWithDebInfo, or MinSizeRel]."
          exit 1
        fi
        cmake_build_type="$2"
        shift
      else
        echo "Error: -j|--jobs requires an argument"
        usage
      fi
      ;;
    *)
      echo "Invalid argument: $1"
      usage
      ;;
    esac
    shift
  done

  if [ -z "${build_dir}" ]; then
    echo "Failure: Missing mandatory argument --build-dir"
    usage
  fi

  if [ -z "${scheduler_type}" ]; then
    echo "Failure: Missing mandatory argument --scheduler-type"
    usage
  fi

  if [ -z "${srpm_dir}" ]; then
    echo "Failure: Missing mandatory argument --srpm-dir"
    usage
  fi

  if [ -z "${vcs_version}" ]; then
    echo "Failure: Missing mandatory argument --vcs-version"
    usage
  fi

  if [ -z "${build_generator}" ]; then
    echo "Failure: Missing mandatory argument --build-generator"
    usage
  fi

  if [ -z "${xrootd_ssi_version}" ]; then
    echo "Failure: Missing mandatory argument --xrootd-ssi-version"
    usage
  fi

  if [ -z "${cmake_build_type}" ]; then
    echo "Failure: Missing mandatory argument --cmake-build-type"
    usage
  fi

  if [ -z "${platform}" ]; then
    echo "Failure: Missing mandatory argument --platform"
    usage
  fi

  cd "${project_root}"
  local cmake_options=""

  if [[ ${clean_build_dir} = true ]]; then
    echo "Removing old build directory: ${build_dir}"
    rm -rf "${build_dir}"
  fi

  if [[ ${create_build_dir} = true ]]; then
    mkdir -p "${build_dir}"
  elif [ ! -d "${build_dir}" ]; then
    echo "Build directory ${build_dir} does not exist. Please create it and execute the script again, or run the script with the --create-build-dir option.."
    exit 1
  fi

  if [ -d "${build_dir}" ] && [ "$(ls -A "${build_dir}")" ]; then
    echo "WARNING: build directory ${build_dir} is not empty"
  fi

  # Setup
  if [ "${install_srpms}" = true ]; then
    ./continuousintegration/utils/project-json/generate_versionlock.py --platform ${platform} >/etc/yum/pluginconf.d/versionlock.list
    if [[ ${use_internal_repos} = true ]]; then
      cp -f continuousintegration/docker/${platform}/etc/yum.repos.d-internal/*.repo /etc/yum.repos.d/
    else
      cp -f continuousintegration/docker/${platform}/etc/yum.repos.d-public/*.repo /etc/yum.repos.d/
    fi
    yum clean all
    yum-builddep --nogpgcheck -y "${srpm_dir}"/*
  fi

  # Cmake
  export CTA_VERSION=${cta_version}

  if [ "${skip_cmake}" = false ]; then
    # Needs to be exported as cmake gets it from the environment
    export XROOTD_SSI_PROTOBUF_INTERFACE_VERSION=${xrootd_ssi_version}

    cmake_options+=" -D VCS_VERSION=${vcs_version}"

    # Build type
    if [[ ! ${cmake_build_type} = "" ]]; then
      echo "Using build type: ${cmake_build_type}"
      cmake_options+=" -D CMAKE_BUILD_TYPE=${cmake_build_type}"
    fi

    # Debug packages
    if [[ ${skip_debug_packages} = true ]]; then
      echo "Skipping debug packages"
      cmake_options+=" -D SKIP_DEBUG_PACKAGES:STRING=1"
    else
      # the else clause is necessary to prevent cmake from caching this variable
      cmake_options+=" -D SKIP_DEBUG_PACKAGES:STRING=0"
    fi

    # Oracle support
    if [[ ${oracle_support} = false ]]; then
      echo "Disabling Oracle Support"
      cmake_options+=" -D DISABLE_ORACLE_SUPPORT:BOOL=ON"
    else
      # the else clause is necessary to prevent cmake from caching this variable
      cmake_options+=" -D DISABLE_ORACLE_SUPPORT:BOOL=OFF"
    fi

    # Unit tests
    if [[ ${skip_unit_tests} = true ]]; then
      echo "Skipping unit tests"
      cmake_options+=" -D SKIP_UNIT_TESTS:STRING=1"
    else
      # the else clause is necessary to prevent cmake from caching this variable
      cmake_options+=" -D SKIP_UNIT_TESTS:STRING=0"
    fi

    # CCache
    if [[ ${enable_ccache} = true ]]; then
      echo "Enabling ccache"
      cmake_options+=" -D ENABLE_CCACHE:STRING=1"
    else
      # the else clause is necessary to prevent cmake from caching this variable
      cmake_options+=" -D ENABLE_CCACHE:STRING=0"
    fi

    # Scheduler type
    if [[ ${scheduler_type} == "pgsched" ]]; then
      echo "Using specified scheduler database type $scheduler_type"
      cmake_options+=" -D CTA_USE_PGSCHED:BOOL=TRUE"
    else
      # unset it
      cmake_options+=" -D CTA_USE_PGSCHED:BOOL=FALSE"
    fi

    cd "${build_dir}"
    echo "Executing cmake..."
    (
      set -x
      cmake3 ${cmake_options} -D JOBS_COUNT:INT=${num_jobs} -G "${build_generator}" "${project_root}"
    )
  else
    echo "Skipping cmake..."
    if [ ! -d "${build_dir}" ]; then
      echo "${build_dir}/ directory does not exist. Ensure to run this script without skipping cmake first."
    fi
    cd "${build_dir}"
  fi

  # Build step
  echo "Executing build step using: ${build_generator}"
  cmake --build . --target cta_rpm -- -j "${num_jobs}"
}

build_rpm "$@"
