#!/bin/bash

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -e

source "$(dirname "${BASH_SOURCE[0]}")/../utils/log_utils.sh"

usage() {
  echo
  echo "Usage: $0 <source|binary|all> [options]"
  echo
  echo "Builds CTA source packages, binary packages, or both in one build directory."
  echo
  echo "Required options:"
  echo "  --build-dir <directory>                 Build root. The detected platform is appended to it."
  echo "  --build-generator <generator>           CMake generator, normally Ninja or Unix Makefiles."
  echo "  --scheduler-type <type>                 Scheduler type: objectstore or pgsched."
  echo "  --cta-version <version>                 CTA version, containing numbers and dots only."
  echo "  --cta-version-suffix <suffix>           CTA version suffix passed to CMake as VCS_VERSION."
  echo "  --cmake-build-type <type>               Release, Debug, RelWithDebInfo, or MinSizeRel."
  echo
  echo "Required for binary and all:"
  echo "  --xrootd-ssi-version <version>          XRootD SSI protobuf interface version."
  echo
  echo "General options:"
  echo "  -j, --jobs <number>                     Number of parallel build jobs."
  echo "      --clean-build-dir                   Remove the build directory before configuring."
  echo "      --create-build-dir                  Create the build directory if it does not exist."
  echo "      --oracle-support <ON|OFF>           Enable or disable Oracle support; default: ON."
  echo "      --extra-telemetry                   Enable performance telemetry instrumentation."
  echo
  echo "Binary package options:"
  echo "      --source-package-dir <directory>    Source packages used to install build dependencies."
  echo "      --install-source-packages           Install dependencies from --source-package-dir."
  echo "      --skip-dependency-install           With all, do not install source-package dependencies."
  echo "      --enable-internal-repos             Enable internal package repositories."
  echo "      --enable-ccache                     Enable ccache."
  echo "      --enable-address-sanitizer          Enable AddressSanitizer."
  echo "      --skip-debug-packages               Do not build native debug packages."
  echo "      --skip-unit-tests                   Do not run unit tests while building packages."
  echo "      --skip-cmake                        Skip configuration for a standalone binary build."
  echo
  echo "The host platform and native package format are detected automatically."
  echo "Currently, only the enterprise Linux backend is implemented."
  echo
}

project_root="$(realpath "$(dirname "${BASH_SOURCE[0]}")/../..")"

command="${1:-}"
if [[ -z "$command" || "$command" == "-h" || "$command" == "--help" ]]; then
  usage
  [[ -n "$command" ]] && exit 0
  exit 1
fi
shift

case "$command" in
  source | binary | all) ;;
  *) die_usage "Unknown package command: $command" ;;
esac

build_root=""
build_generator=""
cta_version=""
cta_version_suffix=""
scheduler_type=""
cmake_build_type=""
xrootd_ssi_version=""
source_package_dir=""

num_jobs=$(nproc --ignore=2)
clean_build_dir=false
create_build_dir=false
oracle_support=true
extra_telemetry=false
install_source_packages=false
skip_dependency_install=false
use_internal_repos=false
enable_ccache=false
enable_address_sanitizer=false
skip_debug_packages=false
skip_unit_tests=false
skip_cmake=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --build-dir | --build-generator | --scheduler-type | --cta-version | --cta-version-suffix | \
      --cmake-build-type | --xrootd-ssi-version | --source-package-dir | -j | --jobs | --oracle-support)
      [[ $# -gt 1 ]] || error_usage "$1 requires an argument"
      option="$1"
      value="$2"
      shift 2

      case "$option" in
        --build-dir) build_root=$(realpath -m "$value") ;;
        --build-generator) build_generator="$value" ;;
        --scheduler-type) scheduler_type="$value" ;;
        --cta-version) cta_version="$value" ;;
        --cta-version-suffix) cta_version_suffix="$value" ;;
        --cmake-build-type) cmake_build_type="$value" ;;
        --xrootd-ssi-version) xrootd_ssi_version="$value" ;;
        --source-package-dir) source_package_dir=$(realpath -m "$value") ;;
        -j | --jobs) num_jobs="$value" ;;
        --oracle-support)
          case "${value,,}" in
            on | true) oracle_support=true ;;
            off | false) oracle_support=false ;;
            *) die_usage "--oracle-support must be ON or OFF" ;;
          esac
          ;;
      esac
      ;;
    --clean-build-dir) clean_build_dir=true; shift ;;
    --create-build-dir) create_build_dir=true; shift ;;
    --extra-telemetry) extra_telemetry=true; shift ;;
    --install-source-packages) install_source_packages=true; shift ;;
    --skip-dependency-install) skip_dependency_install=true; shift ;;
    --enable-internal-repos) use_internal_repos=true; shift ;;
    --enable-ccache) enable_ccache=true; shift ;;
    --enable-address-sanitizer) enable_address_sanitizer=true; shift ;;
    --skip-debug-packages) skip_debug_packages=true; shift ;;
    --skip-unit-tests) skip_unit_tests=true; shift ;;
    --skip-cmake) skip_cmake=true; shift ;;
    -h | --help) usage; exit 0 ;;
    *) die_usage "Invalid argument: $1" ;;
  esac
done

[[ -n "$build_root" ]] || die_usage "Missing mandatory argument --build-dir"
[[ -n "$build_generator" ]] || die_usage "Missing mandatory argument --build-generator"
[[ -n "$scheduler_type" ]] || die_usage "Missing mandatory argument --scheduler-type"
[[ -n "$cta_version" ]] || die_usage "Missing mandatory argument --cta-version"
[[ -n "$cta_version_suffix" ]] || die_usage "Missing mandatory argument --cta-version-suffix"
[[ -n "$cmake_build_type" ]] || die_usage "Missing mandatory argument --cmake-build-type"
[[ "$build_root" != "/" && "$build_root" != "$project_root" ]] \
  || die_usage "--build-dir must not be the filesystem or project root"

[[ "$scheduler_type" == "objectstore" || "$scheduler_type" == "pgsched" ]] \
  || die_usage "--scheduler-type must be objectstore or pgsched"
[[ "$cta_version" =~ ^[0-9.]+$ ]] \
  || die_usage "--cta-version may contain only numbers and dots"
[[ "$cta_version_suffix" =~ ^[a-z0-9.-]+$ ]] \
  || die_usage "--cta-version-suffix may contain only lowercase letters, numbers, dots, and hyphens"
[[ "$cmake_build_type" =~ ^(Release|Debug|RelWithDebInfo|MinSizeRel)$ ]] \
  || die_usage "--cmake-build-type must be Release, Debug, RelWithDebInfo, or MinSizeRel"
[[ "$num_jobs" =~ ^[1-9][0-9]*$ ]] || die_usage "--jobs must be a positive integer"

if [[ "$build_generator" != "Ninja" && "$build_generator" != "Unix Makefiles" ]]; then
  log_warn "Build generator $build_generator is not officially supported. Compilation might not succeed."
fi

if [[ "$command" != "source" && -z "$xrootd_ssi_version" ]]; then
  die_usage "Missing mandatory argument --xrootd-ssi-version"
fi
if [[ "$command" == "source" && "$skip_cmake" == true ]]; then
  die_usage "--skip-cmake is only valid with binary"
fi
if [[ "$command" == "all" && "$skip_cmake" == true ]]; then
  die_usage "--skip-cmake cannot be used with all"
fi
if [[ "$command" == "source" && "$install_source_packages" == true ]]; then
  die_usage "--install-source-packages is only valid with binary"
fi
if [[ "$command" == "binary" && "$install_source_packages" == true && -z "$source_package_dir" ]]; then
  die_usage "--install-source-packages requires --source-package-dir"
fi
if [[ "$command" != "all" && "$skip_dependency_install" == true ]]; then
  die_usage "--skip-dependency-install is only valid with all"
fi

detect_package_backend() {
  local os_id=""
  local os_version_id=""

  [[ -r /etc/os-release ]] || die "Cannot detect the host platform: /etc/os-release is unavailable."
  # os-release is a system-provided shell-compatible data file.
  source /etc/os-release
  os_id="${ID:-}"
  os_version_id="${VERSION_ID:-}"

  case "$os_id" in
    centos | rhel | almalinux | rocky)
      package_format="rpm"
      platform="el${os_version_id%%.*}"
      ;;
    *)
      die "Unsupported host platform ${os_id:-unknown} ${os_version_id:-unknown}. No package backend is implemented for it."
      ;;
  esac

  [[ -d "$project_root/ci/docker/cta/$platform" ]] \
    || die "Detected platform $platform is not supported by this checkout."
}

prepare_build_directory() {
  if [[ "$clean_build_dir" == true ]]; then
    log_task "Removing old build directory ${build_dir}..."
    rm -rf -- "$build_dir"
  fi

  if [[ "$create_build_dir" == true ]]; then
    mkdir -p "$build_dir"
  elif [[ ! -d "$build_dir" ]]; then
    die "Build directory $build_dir does not exist. Create it or use --create-build-dir."
  fi
}

cmake_bool() {
  [[ "$1" == true ]] && echo ON || echo OFF
}

configure_build() {
  local package_mode="$1"
  local build_debug_packages=true
  local run_unit_tests=true
  [[ "$skip_debug_packages" == true ]] && build_debug_packages=false
  [[ "$skip_unit_tests" == true ]] && run_unit_tests=false

  local cmake_options=(
    -D "CTA_PACKAGE_MODE:STRING=${package_mode}"
    -D "VCS_VERSION=${cta_version_suffix}"
    -D "CMAKE_BUILD_TYPE=${cmake_build_type}"
    -D "CTA_WITH_ORACLE:BOOL=$(cmake_bool "$oracle_support")"
    -D "CTA_USE_EXTRA_TELEMETRY:BOOL=$(cmake_bool "$extra_telemetry")"
    -D "CTA_USE_PGSCHED:BOOL=$([[ "$scheduler_type" == pgsched ]] && echo ON || echo OFF)"
    -D "JOBS_COUNT:INT=${num_jobs}"
  )

  if [[ "$package_mode" == "binary" ]]; then
    cmake_options+=(
      -D "CTA_BUILD_DEBUG_PACKAGES:BOOL=$(cmake_bool "$build_debug_packages")"
      -D "CTA_RUN_UNIT_TESTS:BOOL=$(cmake_bool "$run_unit_tests")"
      -D "ENABLE_CCACHE:BOOL=$(cmake_bool "$enable_ccache")"
      -D "ENABLE_ADDRESS_SANITIZER:BOOL=$(cmake_bool "$enable_address_sanitizer")"
    )
    export XROOTD_SSI_PROTOBUF_INTERFACE_VERSION="$xrootd_ssi_version"
  fi

  export CTA_VERSION="$cta_version"
  export GTEST_COLOR=yes

  log_task "Configuring ${package_mode} package build..."
  (
    set -x
    cmake3 "${cmake_options[@]}" -G "$build_generator" -S "$project_root" -B "$build_dir"
  )
}

build_target() {
  local target="$1"
  log_task "Building ${target} with ${build_generator}..."
  cmake --build "$build_dir" --target "$target" --parallel "$num_jobs"
}

source_package_target() {
  case "$package_format" in
    rpm) echo cta_srpm ;;
    *) die "No source-package target is implemented for package format $package_format." ;;
  esac
}

binary_package_target() {
  case "$package_format" in
    rpm) echo cta_rpm ;;
    *) die "No binary-package target is implemented for package format $package_format." ;;
  esac
}

generated_source_package_dir() {
  case "$package_format" in
    rpm) echo "$build_dir/RPM/SRPMS" ;;
    *) die "No source-package output directory is implemented for package format $package_format." ;;
  esac
}

build_source_packages() {
  build_target "$(source_package_target)"
}

build_binary_packages() {
  build_target "$(binary_package_target)"
}

install_rpm_build_dependencies() {
  local srpm_dir="$1"

  [[ -d "$srpm_dir" ]] || die "Source package directory $srpm_dir does not exist."
  compgen -G "$srpm_dir/*.src.rpm" >/dev/null \
    || compgen -G "$srpm_dir/*.rpm" >/dev/null \
    || die "No source RPMs found in $srpm_dir."

  ./ci/project-json/generate_versionlock.py --platform "$platform" >/etc/yum/pluginconf.d/versionlock.list
  cp -f "ci/docker/cta/${platform}/etc/yum.repos.d-public/"*.repo /etc/yum.repos.d/
  if [[ "$use_internal_repos" == true ]]; then
    cp -f "ci/docker/cta/${platform}/etc/yum.repos.d-internal/"*.repo /etc/yum.repos.d/
  fi

  local dnf_cache_dir="${DNF_CACHE_DIR:-/var/cache/dnf}"
  mkdir -p "$dnf_cache_dir"
  dnf builddep --nogpgcheck -y \
    --setopt="cachedir=${dnf_cache_dir}" \
    --setopt=keepcache=1 \
    --setopt=max_parallel_downloads=8 \
    --setopt=metadata_expire=24h \
    "$srpm_dir"/*
}

install_build_dependencies() {
  local package_dir="$1"
  case "$package_format" in
    rpm) install_rpm_build_dependencies "$package_dir" ;;
    *) die "No dependency installer is implemented for package format $package_format." ;;
  esac
}

SECONDS=0
cd "$project_root"
detect_package_backend
build_dir="${build_root}/${platform}"
prepare_build_directory

case "$command" in
  source)
    configure_build source
    build_source_packages
    ;;
  binary)
    if [[ "$install_source_packages" == true ]]; then
      install_build_dependencies "$source_package_dir"
    fi
    if [[ "$skip_cmake" == false ]]; then
      configure_build binary
    else
      [[ -f "$build_dir/CMakeCache.txt" ]] \
        || die "Cannot skip CMake: $build_dir/CMakeCache.txt does not exist."
      log_warn "Skipping CMake configuration."
    fi
    build_binary_packages
    ;;
  all)
    configure_build source
    build_source_packages
    if [[ "$skip_dependency_install" == false ]]; then
      install_build_dependencies "$(generated_source_package_dir)"
    else
      log_warn "Skipping source-package dependency installation."
    fi
    configure_build binary
    build_binary_packages
    ;;
esac

echo
log_success "Completed CTA ${command} package build in ${SECONDS} seconds."
