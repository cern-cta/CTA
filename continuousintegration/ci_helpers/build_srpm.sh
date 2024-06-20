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
  echo "Usage: $0 [options] --build-dir <build-dir> --cta-version <cta-version> --vcs-version <vcs-version> --xrootd-version <xrootd-version> --scheduler-type <scheduler-type>"
  echo ""
  echo "Builds the srpms."
  echo "  --build-dir <build-dir>:              Sets the build directory for the SRPMs. Can be absolute or relative to the repository root."
  echo "  --cta-version <cta-version>:          Sets the CTA_VERSION."
  echo "  --scheduler-type <scheduler-type>:    The scheduler type. Ex: objectstore."
  echo "  --vcs-version <vcs-version>:          Sets the VCS_VERSION variable in cmake."
  echo "  --xrootd-version <xrootd-version>:    Sets the xrootd version. This will also be used as the CTA version. Should be one of [4, 5]."
  echo ""
  echo "options:"
  echo "  -i, --install:                          Installs the required packages. Supported operating systems: [cc7, alma9]."
  echo "  -j, --jobs <num-jobs>:                  How many jobs to use for cmake/make."
  echo "      --skip-unit-tests:                  Skips the unit tests."
  echo "      --oracle-support <ON/OFF>:          When set to OFF, will disable Oracle support. Oracle support is enabled by default."
  exit 1
}

xrootd_supported() {
  [ "$1" -eq 4 ] || [ "$1" -eq 5 ]
}

build_srpm() {

  # Default values for arguments
  local build_dir=""
  local cta_version=""
  local scheduler_type=""
  local vcs_version=""
  local xrootd_version=""

  local install=false
  local num_jobs=1
  local skip_unit_tests=false
  local disable_oracle_support=false

  # Parse command line arguments
  while [[ "$#" -gt 0 ]]; do
    case $1 in
      --build-dir) 
        if [[ $# -gt 1 ]]; then
          build_dir="$2"
          shift
        else
          echo "Error: --build-dir requires an argument"
          usage
        fi
        ;;
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
      -i|--install)
        install=true
        ;;
      -j|--jobs)
        if [[ $# -gt 1 ]]; then
          num_jobs="$2"
          shift
        else
          echo "Error: --jobs requires an argument"
          usage
        fi
        ;;
      --skip-unit-tests) 
        skip_unit_tests=true 
        ;;
      --oracle-support) 
        if [[ $# -gt 1 ]]; then
          if [ "$2" = "OFF" ]; then
            disable_oracle_support=true
          fi
          shift
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
    echo "Please specify --build-dir";
    usage
  fi

  if [ -z "${scheduler_type}" ]; then
    echo "Please specify --scheduler-type";
    usage
  fi  
  
  if [ -z "${vcs_version}" ]; then
    echo "Please specify --vcs-version";
    usage
  fi

  if [ -z "${xrootd_version}" ]; then
    echo "Please specify --xrootd-version";
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

  # Setup
  if [ "${install}" = true ]; then
    echo "Installing prerequisites..."
    if [ -d "${build_dir}" ]; then
      echo "Build directory already exists while asking for install. Attempting removal of existing build directory..."
      rm -r "${build_dir}"
      echo "Old build directory: ${build_dir} removed"
    fi

    # Go through supported Operating Systems
    if [ "$(grep -c 'AlmaLinux release 9' /etc/redhat-release)" -eq 0 ]; then
      # Alma9
      cp -f continuousintegration/docker/ctafrontend/alma9/repos/*.repo /etc/yum.repos.d/
      cp -f continuousintegration/docker/ctafrontend/alma9/yum/pluginconf.d/versionlock.list /etc/yum/pluginconf.d/
      yum install -y epel-release almalinux-release-devel
      yum install -y wget gcc gcc-c++ cmake3 make rpm-build yum-utils
      ./continuousintegration/docker/ctafrontend/alma9/installOracle21.sh
    elif [ "$(grep -c 'CentOS Linux release 7' /etc/redhat-release)" -eq 0 ]; then
      # CentOS 7
      cp -f continuousintegration/docker/ctafrontend/cc7/etc/yum.repos.d/*.repo /etc/yum.repos.d/
      if [[ ${xrootd_version} -eq 4 ]]; then 
        echo "Using XRootD version 4";
        ./continuousintegration/docker/ctafrontend/opt/run/bin/cta-versionlock --file ./continuousintegration/docker/ctafrontend/cc7/etc/yum/pluginconf.d/versionlock.list config xrootd4;
        yum-config-manager --enable cta-ci-xroot;
        yum-config-manager --disable cta-ci-xrootd5;
      else 
        echo "Using XRootD version 5";
      fi
      cp -f continuousintegration/docker/ctafrontend/cc7/etc/yum/pluginconf.d/versionlock.list /etc/yum/pluginconf.d/
      yum install -y devtoolset-11 cmake3 make rpm-build
      source /opt/rh/devtoolset-11/enable
    else
      echo "Failure: Unsupported distribution. Must be one of: [cc7, alma9]"
    fi
  fi

  # Cmake
  export CTA_VERSION=${cta_version}

  cmake_options+=" -DPackageOnly:Bool=true"
  cmake_options+=" -DVCS_VERSION=${vcs_version}"

  if [[ ${disable_oracle_support} = true ]]; then
    echo "Disabling Oracle Support";
    cmake_options+=" -DDISABLE_ORACLE_SUPPORT:BOOL=ON";
  fi

  if [[ ${skip_unit_tests} = true ]]; then
    echo "Skipping unit tests";
    cmake_options+=" -DSKIP_UNIT_TESTS:STRING=1";
  fi

  if [[ ${scheduler_type} != "objectstore" ]]; then
    echo "Using specified scheduler database type $SCHED_TYPE";
    local sched_opt="-DCTA_USE_$(echo "${scheduler_type}" | tr '[:lower:]' '[:upper:]'):Bool=true ";
    cmake_options+=" ${sched_opt}";
  fi

  mkdir -p "${build_dir}"
  cd "${build_dir}"
  echo "Executing cmake..."
  cmake3 ${cmake_options} "${repo_root}"

  # Make
  echo "Executing make..."
  make cta_srpm  -j "${num_jobs}"
}

build_srpm "$@"