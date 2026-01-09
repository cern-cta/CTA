#!/bin/bash

# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -e
source "$(dirname "${BASH_SOURCE[0]}")/../utils/log_wrapper.sh"

usage() {
  echo
  echo "Usage: $0 [options] --build-dir <build-dir> --scheduler-type <scheduler-type> --cta-version <cta-version> --vcs-version <vcs-version>"
  echo
  echo "Builds the srpms."
  echo "  --build-dir <build-dir>:              Sets the build directory for the SRPMs. Can be absolute or relative to where the script is being executed from."
  echo "  --build-generator <generator>:        Specifies the build generator for cmake. Ex: [\"Unix Makefiles\", \"Ninja\"]."
  echo "  --scheduler-type <type>:              The scheduler type. Must be one of [objectstore, pgsched]."
  echo "  --cta-version <cta-version>:          Sets the CTA_VERSION."
  echo "  --vcs-version <vcs-version>:          Sets the VCS_VERSION variable in cmake."
  echo "  --cmake-build-type <type>:            Specifies the build type for cmake. Must be one of [Release, Debug, RelWithDebInfo, or MinSizeRel]."
  echo
  echo "options:"
  echo "  -j, --jobs <num-jobs>:                How many jobs to use for make."
  echo "      --clean-build-dir:                Empties the build directory, ensuring a fresh build from scratch."
  echo "      --create-build-dir                Creates the build directory if it does not exist."
  echo "      --oracle-support <ON/OFF>:        When set to OFF, will disable Oracle support. Oracle support is enabled by default."
  echo
  exit 1
}

build_srpm() {
  project_root="$(realpath "$(dirname "${BASH_SOURCE[0]}")/../..")"
  # Default values for arguments
  local build_dir=""
  local build_generator=""
  local cta_version=""
  local scheduler_type=""
  local vcs_version=""

  local create_build_dir=false
  local clean_build_dir=false
  local num_jobs=$(nproc --ignore=2)
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
        error_usage "--build-dir requires an argument"
      fi
      ;;
    --build-generator)
      if [[ $# -gt 1 ]]; then
        if [[ "$2" != "Ninja" ]] && [[ "$2" != "Unix Makefiles" ]]; then
          echo "Warning: build generator $2 is not officially supported. Compilation might not be successful."
        fi
        build_generator="$2"
        shift
      else
        error_usage "--build-generator requires an argument"
      fi
      ;;
    --clean-build-dir) clean_build_dir=true ;;
    --create-build-dir) create_build_dir=true ;;
    --cta-version)
      if [[ $# -gt 1 ]]; then
        cta_version="$2"
        shift
      else
        error_usage "--cta-version requires an argument"
      fi
      ;;
    --scheduler-type)
      if [[ $# -gt 1 ]]; then
        if [[ "$2" != "objectstore" ]] && [[ "$2" != "pgsched" ]]; then
          error_usage "scheduler type $2 is not one of [objectstore, pgsched]."
        fi
        scheduler_type="$2"
        shift
      else
        error_usage "--scheduler-type requires an argument"
      fi
      ;;
    --vcs-version)
      if [[ $# -gt 1 ]]; then
        vcs_version="$2"
        shift
      else
        error_usage "--vcs-version requires an argument"
      fi
      ;;
    -j | --jobs)
      if [[ $# -gt 1 ]]; then
        num_jobs="$2"
        shift
      else
        error_usage "--jobs requires an argument"
      fi
      ;;
    --oracle-support)
      if [[ $# -gt 1 ]]; then
        if [[ "$2" = "FALSE" ]]; then
          oracle_support=false
        fi
        shift
      fi
      ;;
    --cmake-build-type)
      if [[ $# -gt 1 ]]; then
        if [[ "$2" != "Release" ]] && [[ "$2" != "Debug" ]] && [[ "$2" != "RelWithDebInfo" ]] && [[ "$2" != "MinSizeRel" ]]; then
          die "--cmake-build-type is \"$2\" but must be one of [Release, Debug, RelWithDebInfo, or MinSizeRel]."
        fi
        cmake_build_type="$2"
        shift
      else
        error_usage "-j|--jobs requires an argument"
      fi
      ;;
    *)
      echo "Invalid argument: $1"
      ;;
    esac
    shift
  done

  if [[ -z "${build_dir}" ]]; then
    die_usage "Missing mandatory argument --build-dir"
  fi

  if [[ -z "${scheduler_type}" ]]; then
    die_usage "Missing mandatory argument --scheduler-type"
  fi

  if [[ -z "${vcs_version}" ]]; then
    die_usage "Missing mandatory argument --vcs-version"
  fi

  if [[ -z "${build_generator}" ]]; then
    die_usage "Missing mandatory argument --build-generator"
  fi

  if [[ -z "${cmake_build_type}" ]]; then
    die_usage "Missing mandatory argument --cmake-build-type"
  fi

  cd "${project_root}"
  local cmake_options=""

  if [[ ${clean_build_dir} = true ]]; then
    echo "Removing old build directory: ${build_dir}"
    rm -rf "${build_dir}"
  fi

  if [[ ${create_build_dir} = true ]]; then
    mkdir -p "${build_dir}"
  elif [[ ! -d "${build_dir}" ]]; then
    echo "Build directory ${build_dir} does not exist. Please create it and execute the script again, or run the script with the --create-build-dir option.."
    exit 1
  fi

  if [[ -d "${build_dir}" ]] && [[ "$(ls -A "${build_dir}")" ]]; then
    echo "WARNING: build directory ${build_dir} is not empty"
  fi

  # Cmake
  export CTA_VERSION=${cta_version}

  cmake_options+=" -D PackageOnly:Bool=true"
  cmake_options+=" -D VCS_VERSION=${vcs_version}"

  if [[ ! ${cmake_build_type} = "" ]]; then
    cmake_options+=" -D CMAKE_BUILD_TYPE=${cmake_build_type}"
  fi

  if [[ ${oracle_support} = false ]]; then
    echo "Disabling Oracle Support"
    cmake_options+=" -D DISABLE_ORACLE_SUPPORT:BOOL=ON"
  else
    # the else clause is necessary to prevent cmake from caching this variable
    cmake_options+=" -D DISABLE_ORACLE_SUPPORT:BOOL=OFF"
  fi

  # Scheduler type
  if [[ ${scheduler_type} == "pgsched" ]]; then
    echo "Using specified scheduler database type $scheduler_type"
    cmake_options+=" -D CTA_USE_PGSCHED:BOOL=TRUE"
  else
    # unset it
    cmake_options+=" -D CTA_USE_PGSCHED:BOOL=FALSE"
  fi

  cd "${build_dir}"
  echo "Executing cmake..."
  (
    set -x
    cmake3 ${cmake_options} -D JOBS_COUNT:INT=${num_jobs} -G "${build_generator}" "${project_root}"
  )

  # Build step
  echo "Executing build step using: ${build_generator}"
  cmake --build . --target cta_srpm -- -j "${num_jobs}"
}

build_srpm "$@"
