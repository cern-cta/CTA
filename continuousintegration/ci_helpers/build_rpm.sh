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
  echo "Usage: $0 [options]--build-dir <build-dir> --srpm-dir <srpm-directory> --scheduler-type <scheduler-type> --cta-version <cta-version> --vcs-version <vcs-version> --xrootd-version <xrootd-version> --xrootd-ssi-version <xrootd-ssi-version>"
  echo ""
  echo "Builds the rpms."
  echo "  --build-dir <build-directory>:                Sets the build directory for the RPMs. Can be absolute or relative to where the script is being executed from. Ex: build_rpm"
  echo "  --srpm-dir <srpm-directory>:                  The directory where the source rpms are located. Can be absolute or relative to where the script is being executed from."
  echo "  --scheduler-type <scheduler-type>:            The scheduler type. Ex: objectstore."
  echo "  --cta-version <cta-version>:                  Sets the CTA_VERSION."
  echo "  --vcs-version <vcs-version>:                  Sets the VCS_VERSION variable in cmake."
  echo "  --xrootd-version <xrootd-version>:            Sets the xrootd version. This will also be used as the CTA version. Should be one of [4, 5]."
  echo "  --xrootd-ssi-version <xrootd-ssi-version>:    Sets the XROOTD_SSI_PROTOBUF_INTERFACE_VERSION variable in cmake."
  echo ""
  echo "options:"
  echo "  -i, --install:                                Installs the required packages. Supported operating systems: [cc7, alma9]."
  echo "  -j, --jobs <num_jobs>:                        How many jobs to use for make."
  echo "      --create-build-dir                        Creates the build directory if it does not exist."
  echo "      --skip-cmake                              Skips the cmake step. Can be used if this script is executed multiple times in succession."
  echo "      --skip-unit-tests                         Skips the unit tests. Speeds up the build time by not running the unit tests."
  echo "      --oracle-support <ON/OFF>:                When set to OFF, will disable Oracle support. Oracle support is enabled by default."
  echo "      --cmake-build-type <build-type>:          Specifies the build type for cmake. Must be one of [Release, Debug, RelWithDebInfo, or MinSizeRel]."

  exit 1
}

xrootd_supported() {
  [ "$1" -eq 4 ] || [ "$1" -eq 5 ]
}

build_rpm() {

  # Default values for arguments
  local build_dir=""
  local cta_version=""
  local scheduler_type=""
  local srpm_dir=""
  local vcs_version=""
  local xrootd_version=""
  local xrootd_ssi_version=""

  local install=false
  local num_jobs=1
  local create_build_dir=false
  local skip_cmake=false
  local skip_unit_tests=false
  local oracle_support=true
  local cmake_build_type=""

  # Parse command line arguments
  while [[ "$#" -gt 0 ]]; do
    case $1 in
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
      --create-build-dir)
        create_build_dir=true
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
      --xrootd-version) 
        if [[ $# -gt 1 ]]; then
          xrootd_version="$2"
          shift
        else
          echo "Error: --xrootd-version requires an argument"
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
      -i|--install)
        install=true
        ;;
      -j|--jobs)
        if [[ $# -gt 1 ]]; then
          num_jobs="$2"
          shift
        else
          echo "Error: -j|--jobs requires an argument"
          usage
        fi
        ;;
      --skip-cmake) 
        skip_cmake=true 
        ;;
      --skip-unit-tests) 
        skip_unit_tests=true 
        ;;
      --oracle-support) 
        if [[ $# -gt 1 ]]; then
          if [ "$2" = "OFF" ]; then
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
      -i|--install)
        install=true
        ;;
      -j|--jobs)
        if [[ $# -gt 1 ]]; then
          num_jobs="$2"
          shift
        else
          echo "Error: -j|--jobs requires an argument"
          usage
        fi
        ;;
      --skip-cmake) 
        skip_cmake=true 
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
        else
          echo "Error: -j|--jobs requires an argument"
          usage
        fi
        disable_oracle_support=true 
        ;;
      *)
        echo "Invalid argument: $1"
        usage 
        ;;
    esac
    shift
  done

  if [ -z "${build_dir}" ]; then
    echo "Failure: Missing mandatory argument --build-dir";
    usage
  fi

  if [ -z "${scheduler_type}" ]; then
    echo "Failure: Missing mandatory argument --scheduler-type";
    usage
  fi

  if [ -z "${srpm_dir}" ]; then
    echo "Failure: Missing mandatory argument --srpm-dir";
    usage
  fi

  if [ -z "${vcs_version}" ]; then
    echo "Failure: Missing mandatory argument --vcs-version";
    usage
  fi

  if [ -z "${xrootd_version}" ]; then
    echo "Failure: Missing mandatory argument --xrootd-version";
    usage
  fi

  if ! xrootd_supported "${xrootd_version}"; then 
    echo "Unsupported xrootd-version: ${xrootd_version}. Must be one of [4, 5]."
    exit 1
  fi

  if [ -z "${xrootd_ssi_version}" ]; then
    echo "Failure: Missing mandatory argument --xrootd-ssi-version";
    usage
  fi

  # navigate to root directory
  cd "$(dirname "$0")"
  cd ../../
  local repo_root=$(pwd)
  local cmake_options=""

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
      cp -f continuousintegration/docker/ctafrontend/alma9/repos/*.repo /etc/yum.repos.d/
      cp -f continuousintegration/docker/ctafrontend/alma9/yum/pluginconf.d/versionlock.list /etc/yum/pluginconf.d/
      yum -y install epel-release almalinux-release-devel
      yum -y install wget gcc gcc-c++ cmake3 make rpm-build yum-utils
      yum -y install yum-plugin-versionlock
      ./continuousintegration/docker/ctafrontend/alma9/installOracle21.sh
      yum-builddep --nogpgcheck -y "${srpm_dir}"/*
    elif [ "$(grep -c 'CentOS Linux release 7' /etc/redhat-release)" -eq 1 ]; then
      # CentOS 7
      echo "Found CentOS 7 install..."
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
      yum -y install yum-plugin-priorities yum-plugin-versionlock
      source /opt/rh/devtoolset-11/enable
      yum-builddep --nogpgcheck -y "${srpm_dir}"/*
    else
      echo "Failure: Unsupported distribution. Must be one of: [cc7, alma9]"
    fi
  fi

  if [ -z "${scheduler_type}" ]; then
    echo "Failure: Missing mandatory argument --scheduler-type";
    usage
  fi

  if [ -z "${srpm_dir}" ]; then
    echo "Failure: Missing mandatory argument --srpm-dir";
    usage
  fi

  if [ -z "${vcs_version}" ]; then
    echo "Failure: Missing mandatory argument --vcs-version";
    usage
  fi

  if [ -z "${xrootd_version}" ]; then
    echo "Failure: Missing mandatory argument --xrootd-version";
    usage
  fi

  if ! xrootd_supported "${xrootd_version}"; then 
    echo "Unsupported xrootd-version: ${xrootd_version}. Must be one of [4, 5]."
    exit 1
  fi

  if [ -z "${xrootd_ssi_version}" ]; then
    echo "Failure: Missing mandatory argument --xrootd-ssi-version";
    usage
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
    if [ "$(grep -c 'AlmaLinux release 9' /etc/redhat-release)" -eq 1 ]; then
      # Alma9
      echo "Found Alma 9 install..."
      cp -f continuousintegration/docker/ctafrontend/alma9/repos/*.repo /etc/yum.repos.d/
      cp -f continuousintegration/docker/ctafrontend/alma9/yum/pluginconf.d/versionlock.list /etc/yum/pluginconf.d/
      yum -y install epel-release almalinux-release-devel
      yum -y install wget gcc gcc-c++ cmake3 make rpm-build yum-utils
      yum -y install yum-plugin-versionlock
      ./continuousintegration/docker/ctafrontend/alma9/installOracle21.sh
      yum-builddep --nogpgcheck -y "${srpm_dir}"/*
    elif [ "$(grep -c 'CentOS Linux release 7' /etc/redhat-release)" -eq 1 ]; then
      # CentOS 7
      echo "Found CentOS 7 install..."
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
      yum -y install yum-plugin-priorities yum-plugin-versionlock
      source /opt/rh/devtoolset-11/enable
      yum-builddep --nogpgcheck -y "${srpm_dir}"/*
    else
      echo "Failure: Unsupported distribution. Must be one of: [cc7, alma9]"
    fi
  fi


  # Cmake
  export CTA_VERSION=${cta_version}

  if [ "${skip_cmake}" = false ]; then
    # Needs to be exported as cmake gets it from the environment
    export XROOTD_SSI_PROTOBUF_INTERFACE_VERSION=${xrootd_ssi_version}

    cmake_options+=" -DVCS_VERSION=${vcs_version}"

    if [[ ! ${cmake_build_type} = "" ]]; then
      cmake_options+=" -DCMAKE_BUILD_TYPE=${cmake_build_type}"
    fi

    if [[ ${oracle_support} = false ]]; then
      echo "Disabling Oracle Support";
      cmake_options+=" -DDISABLE_ORACLE_SUPPORT:BOOL=ON";
    fi

    if [[ ${skip_unit_tests} = true ]]; then
      echo "Skipping unit tests";
      cmake_options+=" -DSKIP_UNIT_TESTS:STRING=1";
    fi

    if [[ ${scheduler_type} != "objectstore" ]]; then
      echo "Using specified scheduler database type $SCHED_TYPE";
      local sched_opt=" -DCTA_USE_$(echo "${scheduler_type}" | tr '[:lower:]' '[:upper:]'):Bool=true ";
      cmake_options+=" ${sched_opt}";
    fi

    cd "${build_dir}"
    echo "Executing cmake..."
    echo "${cmake_options}"
    cmake3 ${cmake_options} "${repo_root}"
  else
    echo "Skipping cmake..."
    if [ ! -d "${build_dir}" ]; then
      echo "${build_dir}/ directory does not exist. Ensure to run this script without skipping cmake first."
    fi
    cd "${build_dir}"
  fi

  # Make
  echo "Executing make..."
  make cta_rpm -j "${num_jobs}"
}

build_rpm "$@"
