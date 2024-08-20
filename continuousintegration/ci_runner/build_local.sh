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
  echo "Builds CTA (non-containerized)."
  echo ""
  echo "Usage: $0 [options]"
  echo ""
  echo "options:"
  echo "  -h, --help:                               Shows help output."
  echo "      --build-generator <generator>:        Specifies the build generator for cmake. Supported: [\"Unix Makefiles\", \"Ninja\"]."
  echo "      --clean-build-dir:                    Empties the RPM build directory (build_rpm/ by default), ensuring a fresh build from scratch."
  echo "      --clean-build-dirs:                   Empties both the SRPM and RPM build directories (build_srpm/ and build_rpm/ by default), ensuring a fresh build from scratch."
  echo "      --cmake-build-type <build-type>:      Specifies the build type for cmake. Must be one of [Release, Debug, RelWithDebInfo, or MinSizeRel]."
  echo "      --disable-oracle-support:             Disables support for oracle."
  echo "      --disable-ccache:                     Disables ccache for the building of the rpms."
  echo "      --skip-cmake:                         Skips the cmake step of the build_rpm stage during the build process."
  echo "      --skip-debug-packages                 Skips the building of the debug RPM packages."
  echo "      --skip-unit-tests:                    Skips the unit tests. Speeds up the build time by not running the unit tests."
  echo "      --skip-srpms:                         Skips the building of the SRPMs."
  echo "      --install:                            Installs the required yum packages."
  echo "      --scheduler-type <scheduler-type>:    The scheduler type. Ex: objectstore."
  exit 1
}

compile_deploy() {

  # Input args
  local clean_build_dir=false
  local clean_build_dirs=false
  local skip_cmake=false
  local skip_unit_tests=false
  local skip_debug_packages=false
  local skip_srpms=false
  local build_generator="Ninja"
  local cmake_build_type=""
  local scheduler_type="objectstore"
  local oracle_support="ON"
  local enable_ccache=true
  local install=false

  # Defaults
  local num_jobs=8
  local cta_version="5"
  # These versions don't affect anything functionality wise
  local vcs_version="dev"
  local xrootd_ssi_version="dev"

  # Parse command line arguments
  while [[ "$#" -gt 0 ]]; do
    case $1 in
      -h | --help) usage ;;
      --clean-build-dir) clean_build_dir=true ;;
      --clean-build-dirs) clean_build_dirs=true ;;
      --disable-oracle-support) oracle_support="OFF" ;;
      --disable-ccache) enable_ccache=false ;;
      --skip-cmake) skip_cmake=true ;;
      --skip-unit-tests) skip_unit_tests=true ;;
      --skip-debug-packages) skip_debug_packages=true ;;
      --skip-srpms) skip_srpms=true ;;
      --install) install=true ;;
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

  local initial_loc=$(pwd)
  # Navigate to repo root
  cd "$(dirname "$(realpath "${BASH_SOURCE[0]}")")/../.."

  if [ ${skip_srpms} = false ]; then
    echo "Building SRPMs..."
    local build_srpm_flags=""
    if [[ ${clean_build_dirs} = true ]]; then
      build_srpm_flags+=" --clean-build-dir"
    fi
    if [[ ${install} = true ]]; then
      build_srpm_flags+=" --install"
    fi

    ./continuousintegration/ci_helpers/build_srpm.sh \
      --build-dir build_srpm \
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

  if [ ${skip_cmake} = true ]; then
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
  if [[ ${install} = true ]]; then
    build_rpm_flags+=" --install"
  fi

  echo "Building RPMs..."
  ./continuousintegration/ci_helpers/build_rpm.sh \
    --build-dir build_rpm \
    --build-generator "${build_generator}" \
    --create-build-dir \
    --srpm-dir build_srpm/RPM/SRPMS \
    --cta-version ${cta_version} \
    --vcs-version ${vcs_version} \
    --xrootd-ssi-version ${xrootd_ssi_version} \
    --scheduler-type ${scheduler_type} \
    --oracle-support ${oracle_support} \
    ${build_rpm_flags}

  echo "Build successful"

  cd "${initial_loc}"
}

compile_deploy "$@"
