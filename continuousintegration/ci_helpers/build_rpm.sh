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
  echo "      --vcs-version         Sets the VCS_VERSION variable in cmake."
  echo "      --xrootd-ssi-version  Sets the XROOTD_SSI_PROTOBUF_INTERFACE_VERSION variable in cmake."

  exit 1
}

build_rpm() {

    # navigate to root directory
    cd "$(dirname "$0")"
    cd ../../

    # Default values
    local JOBS=1
    local INSTALL=false
    local DISTRO=""
    local SKIP_CMAKE=false
    local SKIP_UNIT_TESTS=false
    local SRPMS_DIR="build_srpm/RPM/SRPMS"
    local VCS_VERSION="dev"
    local XROOTD_SSI_VERSION="dev"

    # Parse command line arguments
    while [[ "$#" -gt 0 ]]; do
        case $1 in
            -i|--install)
                INSTALL=true
                if [[ $# -gt 1 ]]; then
                    DISTRO=$2
                    shift
                else
                    echo "Error: --install requires an argument"
                    exit 1
                fi
                ;;
            -j|--jobs)
                if [[ $# -gt 1 ]]; then
                    JOBS=$2
                    shift
                else
                    echo "Error: --jobs requires an argument"
                    exit 1
                fi
                ;;
            --skip-cmake) 
                SKIP_CMAKE=true 
                ;;
            --skip-unit-tests) 
                SKIP_UNIT_TESTS=true 
                ;;
            --srpms-dir)
                if [[ $# -gt 1 ]]; then
                    SRPMS_DIR=$2
                    shift
                else
                    echo "Error: --srpms-dir requires an argument"
                    exit 1
                fi
                ;;
            --vcs-version) 
                if [[ $# -gt 1 ]]; then
                    VCS_VERSION=$2
                    shift
                else
                    echo "Error: --vcs-version requires an argument"
                    exit 1
                fi
                ;;
            --xrootd-ssi-version) 
                if [[ $# -gt 1 ]]; then
                    XROOTD_SSI_VERSION=$2
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
    if [ "$INSTALL" = true ]; then
        echo "Installing prerequisites..."
        case $DISTRO in 
            "alma9")
                cp -f continuousintegration/docker/ctafrontend/alma9/repos/*.repo /etc/yum.repos.d/
                cp -f continuousintegration/docker/ctafrontend/alma9/yum/pluginconf.d/versionlock.list /etc/yum/pluginconf.d/
                yum -y install epel-release almalinux-release-devel git
                yum -y install git wget gcc gcc-c++ cmake3 make rpm-build yum-utils
                yum -y install yum-plugin-versionlock
                ./continuousintegration/docker/ctafrontend/alma9/installOracle21.sh
                yum-builddep --nogpgcheck -y $SRPMS_DIR/*
                ;;
            "cc7")
                yum install -y devtoolset-11 cmake3 make rpm-build
                yum -y install yum-plugin-priorities yum-plugin-versionlock
                yum install -y git
                source /opt/rh/devtoolset-11/enable
                yum-builddep --nogpgcheck -y $SRPMS_DIR/*
                ;;
            *)
                echo "Unsupported distribution. Must be one of: [cc7, alma9]"
                exit -1
            ;;
        esac
    fi

    # Cmake
    if [ "$SKIP_CMAKE" = false ]; then
        export XROOTD_SSI_PROTOBUF_INTERFACE_VERSION=$XROOTD_SSI_VERSION

        CMAKE_OPTIONS=" -DVCS_VERSION=${VCS_VERSION}"
        if [ "$SKIP_UNIT_TESTS" = true ]; then
            CMAKE_OPTIONS+=" -DSKIP_UNIT_TESTS:STRING=1"
        fi
        mkdir -p build_rpm
        cd build_rpm
        echo "Executing cmake..."
        cmake3 ${CMAKE_OPTIONS} ..
    else
        echo "Skipping cmake..."
        if [ ! -d build_rpm ]; then
            echo "build_rpm/ directory does not exist. Ensure to run this script without skipping cmake first."
        fi
        cd build_rpm
    fi

    # Make
    echo "Executing make..."
    make cta_rpm -j $JOBS
}

build_rpm "$@"