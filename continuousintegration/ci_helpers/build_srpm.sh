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
  echo ""
  echo "Builds the srpms."
  echo "Usage: $0 [-i|--install <distribution>] [-j|--jobs <num-jobs>] [--skip-cmake] [--vcs-version <vcs-version>]"
  echo ""
  echo "Flags:"
  echo "  -i, --install             Perform the setup and installation part of the required yum packages. Should specify which distribution to use. Should be one of [cc7, alma9]."
  echo "  -j, --jobs                How many jobs to use for cmake/make."
  echo "      --skip-cmake          Skips the cmake step. Can be used if this script is executed multiple times in succession."
  echo "      --vcs-version         Sets the VCS_VERSION variable in cmake. Defaults to \"dev\" if not provided."

  exit 1
}

build_srpm() {

  # navigate to root directory
  cd "$(dirname "$0")"
  cd ../../

  # Default values
  local num_jobs=1
  local install=false
  local distro=""
  local skip_cmake=false
  local vcs_version="dev"

  # Parse command line arguments
  while [[ "$#" -gt 0 ]]; do
    case $1 in
      -i|--install)
        install=true
        if [[ $# -gt 1 ]]; then
          distro="$2"
          shift
        else
          echo "Error: --install requires an argument"
          exit 1
        fi
        ;;
      -j|--jobs)
        if [[ $# -gt 1 ]]; then
          num_jobs="$2"
          shift
        else
          echo "Error: --jobs requires an argument"
          exit 1
        fi
        ;;
      --skip-cmake) 
        skip_cmake=true 
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
        yum install -y epel-release almalinux-release-devel
        yum install -y wget gcc gcc-c++ cmake3 make rpm-build yum-utils
        ./continuousintegration/docker/ctafrontend/alma9/installOracle21.sh
        ;;
      "cc7")
        yum install -y devtoolset-11 cmake3 make rpm-build
        source /opt/rh/devtoolset-11/enable
        ;;
      *)
        echo "Unsupported distribution. Must be one of: [cc7, alma9]"
        exit -1
      ;;
    esac
  fi

  # Cmake
  if [ "${skip_cmake}" = false ]; then
    cmake_options=" -DVCS_VERSION=${vcs_version}"

    mkdir -p build_srpm
    cd build_srpm
    echo "Executing cmake..."
    cmake3 -DPackageOnly:Bool=true ${cmake_options} ..
  else
    echo "Skipping cmake..."
    # build_srpm should exist
    if [ ! -d build_srpm ]; then
      echo "build_srpm/ directory does not exist. Ensure to run this script without skipping cmake first."
    fi
    cd build_srpm
  fi

  # Make
  echo "Executing make..."
  make cta_srpm  -j ${num_jobs}
}

build_srpm "$@"