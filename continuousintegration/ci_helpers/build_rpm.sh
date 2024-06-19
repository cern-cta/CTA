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
  echo "Builds the rpms."
  echo ""
  echo "Usage: $0 [-i|--install <distribution>] [-j|--jobs <num-jobs>] [--skip-cmake] [--skip-unit-tests] [--srpms-dir <srpms-directory>] [--vcs-version <vcs-version>] [--xrootd-ssi-version <xrootd-ssi-version>]"
  echo ""
  echo "Flags:"
  echo "  -i, --install             Perform the setup and installation part of the required yum packages. Should specify which distribution to use. Should be one of [cc7, alma9]."
  echo "  -j, --jobs                How many jobs to use for cmake/make."
  echo "      --skip-cmake          Skips the cmake step. Can be used if this script is executed multiple times in succession."
  echo "      --skip-unit-tests     Skips the unit tests. Speeds up the build time by not running the unit tests."
  echo "      --srpms-dir           The directory where the source rpms are located. Defaults to build_srpm/RPM/SRPMS (i.e. the output directory of the build_srpm.sh script). Note that this directory is relative to the root of the repository."
  echo "      --vcs-version         Sets the VCS_VERSION variable in cmake. Defaults to \"dev\" if not provided."
  echo "      --xrootd-ssi-version  Sets the XROOTD_SSI_PROTOBUF_INTERFACE_VERSION variable in cmake. If not provided, this will keep the existing XROOTD_SSI_PROTOBUF_INTERFACE_VERSION env variable if set, and otherwise will set it to \"dev\"."

  exit 1
}

build_rpm() {

  # navigate to root directory
  cd "$(dirname "$0")"
  cd ../../

  # Default values
  local num_jobs=1
  local install=false
  local distro=""
  local skip_cmake=false
  local skip_unit_tests=false
  local srpms_dir="build_srpm/RPM/SRPMS"
  local vcs_version="dev"
  local xrootd_ssi_version="${XROOTD_SSI_PROTOBUF_INTERFACE_VERSION:-dev}"

  # Parse command line arguments
  while [[ "$#" -gt 0 ]]; do
    case $1 in
      -i|--install)
        install=true
        if [[ $# -gt 1 ]]; then
          distro="$2"
          shift
        else
          echo "Error: -i|--install requires an argument"
          exit 1
        fi
        ;;
      -j|--jobs)
        if [[ $# -gt 1 ]]; then
          num_jobs="$2"
          shift
        else
          echo "Error: -j|--jobs requires an argument"
          exit 1
        fi
        ;;
      --skip-cmake) 
        skip_cmake=true 
        ;;
      --skip-unit-tests) 
        skip_unit_tests=true 
        ;;
      --srpms-dir)
        if [[ $# -gt 1 ]]; then
            srpms_dir="$2"
            shift
        else
            echo "Error: --srpms-dir requires an argument"
            exit 1
        fi
        ;;
      --vcs-version) 
        if [[ $# -gt 1 ]]; then
          vcs_version="$2"
          shift
        else
          echo "Error: --vcs-version requires an argument"
          exit 1
        fi
        ;;
      --xrootd-ssi-version) 
        if [[ $# -gt 1 ]]; then
          xrootd_ssi_version="$2"
          shift
        else
          echo "Error: --vcs-version requires an argument"
          exit 1
        fi
        ;;
      *)
        echo "Invalid argument: $1"
        usage 
        ;;
    esac
    shift
  done

  # Setup
  if [ "${install}" = true ]; then
    echo "Installing prerequisites..."
    case ${distro} in 
      "alma9")
        cp -f continuousintegration/docker/ctafrontend/alma9/repos/*.repo /etc/yum.repos.d/
        cp -f continuousintegration/docker/ctafrontend/alma9/yum/pluginconf.d/versionlock.list /etc/yum/pluginconf.d/
        yum -y install epel-release almalinux-release-devel
        yum -y install wget gcc gcc-c++ cmake3 make rpm-build yum-utils
        yum -y install yum-plugin-versionlock
        ./continuousintegration/docker/ctafrontend/alma9/installOracle21.sh
        yum-builddep --nogpgcheck -y ${srpms_dir}/*
        ;;
      "cc7")
        yum install -y devtoolset-11 cmake3 make rpm-build
        yum -y install yum-plugin-priorities yum-plugin-versionlock
        source /opt/rh/devtoolset-11/enable
        yum-builddep --nogpgcheck -y ${srpms_dir}/*
        ;;
      *)
        echo "Unsupported distribution. Must be one of: [cc7, alma9]"
        exit -1
      ;;
    esac
  fi

  # Cmake
  if [ "$skip_cmake" = false ]; then
    # Needs to be exported as cmake gets it from the environment
    export XROOTD_SSI_PROTOBUF_INTERFACE_VERSION=${xrootd_ssi_version}

    cmake_options=" -DVCS_VERSION=${vcs_version}"
    if [ "${skip_unit_tests}" = true ]; then
      cmake_options+=" -Dskip_unit_tests:STRING=1"
    fi
    mkdir -p build_rpm
    cd build_rpm
    echo "Executing cmake..."
    cmake3 ${cmake_options} ..
  else
    echo "Skipping cmake..."
    if [ ! -d build_rpm ]; then
      echo "build_rpm/ directory does not exist. Ensure to run this script without skipping cmake first."
    fi
    cd build_rpm
  fi

  # Make
  echo "Executing make..."
  make cta_rpm -j ${num_jobs}
}

build_rpm "$@"