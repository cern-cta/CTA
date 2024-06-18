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

set -e

# navigate to root directory
cd "$(dirname "$0")"
cd ../../

usage() {
  echo "Builds the rpms."
  echo ""
  echo "Usage: $0 [-i] [-p] [-j <num-jobs>] [--build-dep] [--srpms-dir <srpms-directory>] [--skip-cmake] [--skip-unit-tests] "
  echo ""
  echo "Flags:"
  echo "  -i, --install             Perform the setup and installation part of the required yum packages."
  echo "  -p, --pipeline            Sets some options to make this script suited for execution in a pipeline."
  echo "  -j, --jobs                How many jobs to use for cmake/make."
  echo "      --build-dep           Executes yum-builddep on the SRPMS_DIR. Note that this flag is redundant when the -i flag is enabled."
  echo "      --srpms-dir           The directory where the source rpms are located. Defaults to build_srpm/RPM/SRPMS (i.e. the output directory of the build_srpm.sh script). Note that this directory is relative to the root of the repository."
  echo "      --skip-cmake          Skips the cmake step. Can be used if this script is executed multiple times in succession."
  echo "      --skip-unit-tests     Skips the unit tests. Speeds up the build time by not running the unit tests."

  exit 1
}

# Default values
JOBS=1
INSTALL=false
BUILD_DEP=false
PIPELINE=false
SKIP_CMAKE=false
SRPMS_DIR=build_srpm/RPM/SRPMS
SKIP_UNIT_TESTS=false

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
  case $1 in
    -i  |--install) INSTALL=true ;;
    -p  |--pipeline) PIPELINE=true ;;
    -j  |--jobs) JOBS="$2" shift ;;
    --build-dep) BUILD_DEP=true ;;
    --srpms-dir) SRPMS_DIR="$2" shift ;;
    --skip-cmake) SKIP_CMAKE=true ;;
    --skip-unit-tests) SKIP_UNIT_TESTS=true ;;
    *)
    usage ;;
  esac
  shift
done

# Setup
if [ "$INSTALL" = true ]; then
    echo "Installing prerequisites..."
    cp -f continuousintegration/docker/ctafrontend/alma9/repos/*.repo /etc/yum.repos.d/
    cp -f continuousintegration/docker/ctafrontend/alma9/yum/pluginconf.d/versionlock.list /etc/yum/pluginconf.d/
    yum -y install epel-release almalinux-release-devel git
    yum -y install git wget gcc gcc-c++ cmake3 make rpm-build yum-utils
    yum -y install yum-plugin-versionlock
    ./continuousintegration/docker/ctafrontend/alma9/installOracle21.sh
fi

if [ "$INSTALL" = true ] || [ "$BUILD_DEP" = true ]; then
    yum-builddep --nogpgcheck -y $SRPMS_DIR/*
fi

# Cmake
if [ "$SKIP_CMAKE" = false ]; then

    if [ "$SKIP_UNIT_TESTS" = true ]; then
        CMAKE_OPTIONS+=" -DSKIP_UNIT_TESTS:STRING=1"
    fi

    if [ "$PIPELINE" = true ]; then
        CMAKE_OPTIONS+=" -DVCS_VERSION=${CTA_BUILD_ID}"
        cd xrootd-ssi-protobuf-interface && export XROOTD_SSI_PROTOBUF_INTERFACE_VERSION=$(git describe --tags --abbrev=0) && cd ..
    fi
    mkdir -p build_rpm
    cd build_rpm
    echo "Executing cmake..."
    cmake3 ${CMAKE_OPTIONS} ..
else
    echo "Skipping cmake..."
    # build_rpm should exist
    if [ ! -d build_rpm ]; then
        echo "build_rpm/ directory does not exist. Ensure to run this script without skipping cmake first."
    fi
    cd build_rpm
fi

# Make
echo "Executing make..."
make cta_rpm -j $JOBS
