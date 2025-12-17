#!/bin/bash

# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -e
source "$(dirname "${BASH_SOURCE[0]}")/../utils/log_wrapper.sh"

# Help message
usage() {
  echo
  echo "Builds CTA (non-containerized)."
  echo
  echo "Usage: $0 [options]"
  echo
  echo "options:"
  echo "  -h, --help:                           Shows help output."
  echo "      --build-generator <generator>:    Specifies the build generator for cmake. Supported: [\"Unix Makefiles\", \"Ninja\"]."
  echo "      --clean-build-dir:                Empties the RPM build directory (build_rpm/ by default), ensuring a fresh build from scratch."
  echo "      --clean-build-dirs:               Empties both the SRPM and RPM build directories (build_srpm/ and build_rpm/ by default), ensuring a fresh build from scratch."
  echo "      --cmake-build-type <type>:        Specifies the build type for cmake. Must be one of [Release, Debug, RelWithDebInfo, or MinSizeRel]."
  echo "      --disable-oracle-support:         Disables support for oracle."
  echo "      --disable-ccache:                 Disables ccache for the building of the rpms."
  echo "      --skip-cmake:                     Skips the cmake step of the build_rpm stage during the build process."
  echo "      --skip-debug-packages             Skips the building of the debug RPM packages."
  echo "      --skip-unit-tests:                Skips the unit tests. Speeds up the build time by not running the unit tests."
  echo "      --skip-srpms:                     Skips the building of the SRPMs."
  echo "      --install <platform>:             Installs the required yum packages for the given platform."
  echo "      --scheduler-type <type>:          The scheduler type. Ex: objectstore."
  echo
  exit 1
}

build_local() {
  project_root="$(realpath "$(dirname "$0")/../..")"
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
  local oracle_support="TRUE"
  local enable_ccache=true
  local install=false
  local install_platform=""

  # Defaults
  local num_jobs=8
  local cta_version="5"
  # These versions don't affect anything functionality wise
  local vcs_version="dev"
  local xrootd_ssi_version="dev"

  # Parse command line arguments
  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      -h | --help) usage ;;
      --clean-build-dir) clean_build_dir=true ;;
      --clean-build-dirs) clean_build_dirs=true ;;
      --disable-oracle-support) oracle_support="FALSE" ;;
      --disable-ccache) enable_ccache=false ;;
      --skip-cmake) skip_cmake=true ;;
      --skip-unit-tests) skip_unit_tests=true ;;
      --skip-debug-packages) skip_debug_packages=true ;;
      --skip-srpms) skip_srpms=true ;;
      --install)
        if [[ $# -gt 1 ]]; then
          install_platform="$2"
          shift
        else
          echo "Error: --install requires an argument"
          usage
        fi
        ;;
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
          if [[ "$2" != "Release" ]] && [[ "$2" != "Debug" ]] && [[ "$2" != "RelWithDebInfo" ]] && [[ "$2" != "MinSizeRel" ]]; then
            echo "--cmake-build-type is \"$2\" but must be one of [Release, Debug, RelWithDebInfo, or MinSizeRel]."
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
        echo "Unsupported argument: $1"
        usage
        ;;
    esac
    shift
  done

  # Navigate to repo root
  cd "${project_root}"

  if [[ ${skip_srpms} = false ]]; then
    echo "Building SRPMs..."
    local build_srpm_flags=""
    if [[ ${clean_build_dirs} = true ]]; then
      build_srpm_flags+=" --clean-build-dir"
    fi
    if [[ ${install} = true ]]; then
      if [[ ${install_platform} == "el9" ]]; then
        dnf install -y epel-release almalinux-release-devel git python3-dnf-plugin-versionlock
        dnf install -y gcc gcc-c++ cmake3 rpm-build dnf-utils pandoc which make ninja-build ccache systemd-devel
      else
        echo "Platform not supported: ${install_platform}. Must be one of [el9]"
      fi
    fi

    ./continuousintegration/build/build_srpm.sh \
      --build-dir build_srpm \
      --build-generator "${build_generator}" \
      --create-build-dir \
      --cta-version ${cta_version} \
      --vcs-version ${vcs_version} \
      --scheduler-type ${scheduler_type} \
      --oracle-support ${oracle_support} \
      --jobs ${num_jobs} \
      ${build_srpm_flags}
  fi

  echo "Compiling the CTA project from source directory"

  local build_rpm_flags="--jobs ${num_jobs}"

  if [[ ${skip_cmake} = true ]]; then
    build_rpm_flags+=" --skip-cmake"
  fi
  if [[ ${skip_unit_tests} = true ]]; then
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

  echo "Building RPMs..."
  ./continuousintegration/build/build_rpm.sh \
    --build-dir build_rpm \
    --build-generator "${build_generator}" \
    --create-build-dir \
    --srpm-dir build_srpm/RPM/SRPMS \
    --cta-version ${cta_version} \
    --vcs-version ${vcs_version} \
    --xrootd-ssi-version ${xrootd_ssi_version} \
    --scheduler-type ${scheduler_type} \
    --oracle-support ${oracle_support} \
    --install-srpms \
    --platform ${install_platform} \
    ${build_rpm_flags}

  echo "Build successful"

  cd "${initial_loc}"
}

build_local "$@"
