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
  echo "Usage: $0 [options] --build-dir <build-dir> --scheduler-type <scheduler-type> --cta-version <cta-version> --vcs-version <vcs-version> --xrootd-version <xrootd-version> "
  echo ""
  echo "Builds the srpms."
  echo "  --build-dir <build-dir>:              Sets the build directory for the SRPMs. Can be absolute or relative to where the script is being executed from."
  echo "  --build-generator <generator>:        Specifies the build generator for cmake. Ex: [\"Unix Makefiles\", \"Ninja\"]."
  echo "  --scheduler-type <scheduler-type>:    The scheduler type. Ex: objectstore."
  echo "  --cta-version <cta-version>:          Sets the CTA_VERSION."
  echo "  --vcs-version <vcs-version>:          Sets the VCS_VERSION variable in cmake."
  echo "  --xrootd-version <xrootd-version>:    Sets the xrootd version. This will also be used as the CTA version. Should be one of [4, 5]."
  echo ""
  echo "options:"
  echo "  -i, --install:                        Installs the required packages. Supported operating systems: [cc7, alma9]."
  echo "  -j, --jobs <num-jobs>:                How many jobs to use for make."
  echo "      --clean-build-dir:                Empties the build directory, ensuring a fresh build from scratch."
  echo "      --create-build-dir                Creates the build directory if it does not exist."
  echo "      --skip-unit-tests:                Skips the unit tests."
  echo "      --oracle-support <ON/OFF>:        When set to OFF, will disable Oracle support. Oracle support is enabled by default."
  echo "      --cmake-build-type <build-type>:  Specifies the build type for cmake. Must be one of [Release, Debug, RelWithDebInfo, or MinSizeRel]."
  exit 1
}

xrootd_supported() {
  [ "$1" -eq 4 ] || [ "$1" -eq 5 ]
}

build_srpm() {

  # Default values for arguments
  local build_dir=""
  local build_generator=""
  local cta_version=""
  local scheduler_type=""
  local vcs_version=""
  local xrootd_version=""

  local create_build_dir=false
  local clean_build_dir=false
  local install=false
  local num_jobs=1
  local skip_unit_tests=false
  local oracle_support=true
  local cmake_build_type=""

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
          scheduler_type="$2"
          shift
        else
          echo "Error: --scheduler-type requires an argument"
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
      --xrootd-version)
        if [[ $# -gt 1 ]]; then
          xrootd_version="$2"
          shift
        else
          echo "Error: --xrootd-version requires an argument"
          usage
        fi
        ;;
      -i | --install) install=true ;;
      -j | --jobs)
        if [[ $# -gt 1 ]]; then
          num_jobs="$2"
          shift
        else
          echo "Error: --jobs requires an argument"
          usage
        fi
        ;;
      --skip-unit-tests) skip_unit_tests=true ;;
      --oracle-support)
        if [[ $# -gt 1 ]]; then
          if [ "$2" = "OFF" ]; then
            oracle_support=false
          fi
          shift
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

  if [ -z "${vcs_version}" ]; then
    echo "Failure: Missing mandatory argument --vcs-version"
    usage
  fi

  if [ -z "${xrootd_version}" ]; then
    echo "Failure: Missing mandatory argument --xrootd-version"
    usage
  fi

  if [ -z "${build_generator}" ]; then
    echo "Failure: Missing mandatory argument --build-generator";
    usage
  fi

  if ! xrootd_supported "${xrootd_version}"; then
    echo "Unsupported xrootd-version: ${xrootd_version}. Must be one of [4, 5]."
    exit 1
  fi

  # navigate to root directory
  cd "$(dirname "$0")"
  cd ../../
  local repo_root=$(pwd)
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

  if [ -n "${build_dir}" ]; then
    echo "WARNING: build directory ${build_dir} is not empty"
  fi

  # Setup
  if [ "${install}" = true ]; then
    echo "Installing prerequisites..."

    # Go through supported Operating Systems
    if [ "$(grep -c 'AlmaLinux release 9' /etc/redhat-release)" -eq 1 ]; then
      # Alma9
      echo "Found Alma 9 install..."
      cp -f continuousintegration/docker/ctafrontend/alma9/etc/yum.repos.d/*.repo /etc/yum.repos.d/
      cp -f continuousintegration/docker/ctafrontend/alma9/etc/yum/pluginconf.d/versionlock.list /etc/yum/pluginconf.d/
      yum install -y epel-release almalinux-release-devel
      yum install -y wget gcc gcc-c++ cmake3 rpm-build yum-utils
      case "${build_generator}" in
        "Unix Makefiles")
          yum install -y make
          ;;
        "Ninja")
          yum install -y ninja-build
          ;;
        *)
          echo "Failure: Unsupported build generator for alma9: ${build_generator}"
          exit 1
          ;;
      esac
      ./continuousintegration/docker/ctafrontend/alma9/installOracle21.sh
    elif [ "$(grep -c 'CentOS Linux release 7' /etc/redhat-release)" -eq 1 ]; then
      # CentOS 7
      echo "Found CentOS 7 install..."
      if [[ ! ${build_generator} = "Unix Makefiles" ]]; then
        # We only support Unix Makefiles for cc7
        echo "Failure: Unsupported build generator for cc7: ${build_generator}"
        exit 1
      fi
      cp -f continuousintegration/docker/ctafrontend/cc7/etc/yum.repos.d/*.repo /etc/yum.repos.d/
      if [[ ${xrootd_version} -eq 4 ]]; then
        echo "Using XRootD version 4"
        ./continuousintegration/docker/ctafrontend/opt/run/bin/cta-versionlock --file ./continuousintegration/docker/ctafrontend/cc7/etc/yum/pluginconf.d/versionlock.list config xrootd4
        yum-config-manager --enable cta-ci-xroot
        yum-config-manager --disable cta-ci-xrootd5
      else
        echo "Using XRootD version 5"
      fi
      cp -f continuousintegration/docker/ctafrontend/cc7/etc/yum/pluginconf.d/versionlock.list /etc/yum/pluginconf.d/
      yum install -y devtoolset-11 cmake3 make rpm-build
      source /opt/rh/devtoolset-11/enable
    else
      echo "Failure: Unsupported distribution. Must be one of: [cc7, alma9]"
      exit 1
    fi
  fi

  # Cmake
  export CTA_VERSION=${cta_version}

  cmake_options+=" -DPackageOnly:Bool=true"
  cmake_options+=" -DVCS_VERSION=${vcs_version}"

  if [[ ! ${cmake_build_type} = "" ]]; then
    cmake_options+=" -DCMAKE_BUILD_TYPE=${cmake_build_type}"
  fi

  if [[ ${oracle_support} = false ]]; then
    echo "Disabling Oracle Support"
    cmake_options+=" -DDISABLE_ORACLE_SUPPORT:BOOL=ON"
  fi

  if [[ ${skip_unit_tests} = true ]]; then
    echo "Skipping unit tests"
    cmake_options+=" -DSKIP_UNIT_TESTS:STRING=1"
  fi

  if [[ ${scheduler_type} != "objectstore" ]]; then
    echo "Using specified scheduler database type $SCHED_TYPE"
    local sched_opt=" -DCTA_USE_$(echo "${scheduler_type}" | tr '[:lower:]' '[:upper:]'):Bool=true "
    cmake_options+=" ${sched_opt}"
  fi

  cd "${build_dir}"
  echo "Executing cmake..."
  (set -x; cmake3 ${cmake_options} -G "${build_generator}" "${repo_root}")

  # Build step
  echo "Executing build step using: ${build_generator}"
  cmake --build . --target cta_srpm -- -j "${num_jobs}"
}

build_srpm "$@"
