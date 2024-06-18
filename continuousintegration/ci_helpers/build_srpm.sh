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
  echo "Builds the srpms."
  echo ""
  echo "Usage: $0 [-i] [-p] [-j <num-jobs>] [--skip-cmake]"
  echo ""
  echo "Flags:"
  echo "  -i, --install       Perform the setup and installation part of the required yum packages."
  echo "  -p, --pipeline      Sets some options to make this script suited for execution in a pipeline."
  echo "  -j, --jobs          How many jobs to use for cmake/make."
  echo "      --skip-cmake    Skips the cmake step. Can be used if this script is executed multiple times in succession."

  exit 1
}

# Default values
JOBS=1
INSTALL=false
PIPELINE=false
SKIP_CMAKE=false

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
  case $1 in
    -i  |--install) INSTALL=true ;;
    -p  |--pipeline) PIPELINE=true ;;
    -j  |--jobs) JOBS="$2" shift ;;
    --skip-cmake) SKIP_CMAKE=true ;;
    *)
    usage ;;
  esac
  shift
done

# Setup
if [ "$INSTALL" = true ]; then
    cp -f continuousintegration/docker/ctafrontend/alma9/repos/*.repo /etc/yum.repos.d/
    cp -f continuousintegration/docker/ctafrontend/alma9/yum/pluginconf.d/versionlock.list /etc/yum/pluginconf.d/
    yum -y install epel-release almalinux-release-devel git
    yum -y install git wget gcc gcc-c++ cmake3 make rpm-build yum-utils
    ./continuousintegration/docker/ctafrontend/alma9/installOracle21.sh
fi

# Cmake
if [ "$SKIP_CMAKE" = false ]; then
    if [ "$PIPELINE" = true ]; then
        CMAKE_OPTIONS+=" -DVCS_VERSION=${CTA_BUILD_ID}"
    fi

    mkdir -p build_srpm
    cd build_srpm
    echo "Executing cmake..."
    cmake3 -DPackageOnly:Bool=true ${CMAKE_OPTIONS} ..
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
make cta_srpm  -j $JOBS
