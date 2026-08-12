#!/bin/bash

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -eo pipefail

# =========================================================================
#  Configuration
# =========================================================================

# Constants
script_dir="$(dirname "$(realpath "${BASH_SOURCE[0]}")")"
readonly script_dir

for required_command in jq git python3; do
  if ! command -v "$required_command" >/dev/null 2>&1 || ! "$required_command" --version >/dev/null 2>&1; then
    echo "ERROR: Required command '$required_command' is missing or unusable." >&2
    exit 1
  fi
done
if ! project_root=$(git -C "$script_dir" rev-parse --show-toplevel 2>/dev/null); then
  echo "ERROR: Could not locate the CTA Git worktree containing '$script_dir'." >&2
  exit 1
fi
readonly project_root

# shellcheck disable=SC2207
readonly available_tests=(
  setup
  verification
  teardown
  $(for f in ${project_root}/ci/system_tests/tests/*_test.py; do basename "$f" _test.py; done)
)
readonly venv_dir="${project_root}/ci/system_tests/.venv"
readonly program_name="cta-dev"
readonly build_state_schema_version=1
readonly build_state_file="${project_root}/build_rpm/.cta-dev-build-state.json"

# Global
platform=$(jq -r .dev.defaultPlatform "${project_root}/project.json")
scheduler_type="objectstore"
oracle_support="true"
enable_internal_repos=true
container_runtime=""
container_runtime_explicit=false
cta_image_tag=dev-0
deploy_namespace="dev"

# Build
build_container_restarted=false
reset=false
clean_build_dir=false
clean_build_dirs=false
enable_ccache=true
skip_debug_packages=false
skip_unit_tests=true
force_install=false
enable_address_sanitizer=false
build_generator="Ninja"
cmake_build_type=$(jq -r .dev.defaultBuildType "${project_root}/project.json")

# Images
image_cleanup=true
enable_debug_image=false

# Deploy
dcache_enabled=false
eos_enabled=true
local_telemetry=false
publish_telemetry=false
eos_image_repository=""
eos_image_tag=""
catalogue_config="presets/dev-catalogue-postgres-values.yaml"
scheduler_config=""
cta_config=""
eos_config=""
extra_spawn_options=(--no-setup)

stage_names=()
stage_durations=()

source "${script_dir}/utils/log_utils.sh"

load_cta_dev_env() {
  local -r env_file="${script_dir}/.cta-dev.env"
  [[ -f "$env_file" ]] || return 0

  local line key value quote
  local line_number=0
  while IFS= read -r line || [[ -n "$line" ]]; do
    line_number=$((line_number + 1))
    line=${line%$'\r'}
    [[ "$line" =~ ^[[:space:]]*(#.*)?$ ]] && continue

    if [[ ! "$line" =~ ^[[:space:]]*([A-Z][A-Z0-9_]*)[[:space:]]*=(.*)$ ]]; then
      die "Invalid configuration in ${env_file}:${line_number}: expected KEY=VALUE."
    fi

    key=${BASH_REMATCH[1]}
    value=${BASH_REMATCH[2]}
    value=${value#"${value%%[![:space:]]*}"}
    value=${value%"${value##*[![:space:]]}"}

    quote=${value:0:1}
    if [[ "$quote" == '"' || "$quote" == "'" ]]; then
      if [[ ${#value} -lt 2 || "${value: -1}" != "$quote" ]]; then
        die "Invalid configuration in ${env_file}:${line_number}: unmatched quote."
      fi
      value=${value:1:${#value}-2}
    elif [[ "$value" == *'"'* || "$value" == *"'"* ]]; then
      die "Invalid configuration in ${env_file}:${line_number}: unmatched quote."
    fi

    case "$key" in
      CTA_DEV_SCHEDULER_TYPE)
        [[ "$value" == objectstore || "$value" == pgsched ]] || \
          die "Invalid value for ${key} in ${env_file}:${line_number}: expected objectstore or pgsched."
        scheduler_type=$value
        ;;
      CTA_DEV_ORACLE_SUPPORT)
        [[ "$value" == true || "$value" == false ]] || \
          die "Invalid value for ${key} in ${env_file}:${line_number}: expected true or false."
        oracle_support=$value
        ;;
      CTA_DEV_INTERNAL_REPOS)
        [[ "$value" == true || "$value" == false ]] || \
          die "Invalid value for ${key} in ${env_file}:${line_number}: expected true or false."
        enable_internal_repos=$value
        ;;
      CTA_DEV_PLATFORM)
        jq -e --arg platform "$value" '.platforms | has($platform)' "${project_root}/project.json" >/dev/null || \
          die "Invalid value for ${key} in ${env_file}:${line_number}: unsupported platform '${value}'."
        platform=$value
        ;;
      CTA_DEV_BUILD_GENERATOR)
        [[ "$value" == Ninja || "$value" == "Unix Makefiles" ]] || \
          die "Invalid value for ${key} in ${env_file}:${line_number}: expected Ninja or Unix Makefiles."
        build_generator=$value
        ;;
      CTA_DEV_CMAKE_BUILD_TYPE)
        [[ "$value" == Release || "$value" == Debug || "$value" == RelWithDebInfo || "$value" == MinSizeRel ]] || \
          die "Invalid value for ${key} in ${env_file}:${line_number}: expected Release, Debug, RelWithDebInfo, or MinSizeRel."
        cmake_build_type=$value
        ;;
      *)
        die "Unknown configuration key '${key}' in ${env_file}:${line_number}."
        ;;
    esac
  done < "$env_file"
}

# =========================================================================
#  Help
# =========================================================================

usage() {
  cat <<EOF

Developer workflow utility for CTA. This script orchestrates the local
development workflow for CTA. Commands can be executed independently or
combined into a complete development pipeline.

Per-worktree defaults can be configured in ${script_dir}/.cta-dev.env.
Command-line options override configured defaults.

Usage:
  $(basename "$0") <command> [global-options] [command-options]

Commands:
  build      Build the CTA RPMs inside a persistent build container.
  images     Build container images from the generated RPMs.
  deploy     Deploy a local CTA development instance.
  test       Run a system test.
  up         Equivalent to: build > images > deploy.
  all        Equivalent to: build > images > deploy > test.

  install    Creates a symlink to invoke this script using '$program_name'.
  help       Show this help.

Global options:
  -h, --help                         Show this help.
      --container-runtime <runtime>  Container runtime [docker, podman].
                                     Auto-detected (working Podman preferred).
                                     Docker requires the Buildx plugin.
      --platform <platform>          Platform to build for.
                                     Defaults to project.json.
      --scheduler-type <type>        Scheduler backend
                                     [objectstore, pgsched].
      --disable-oracle-support       Build without Oracle support.
      --use-public-repos             Use public YUM repositories instead of
                                     CERN internal repositories.

Run:

  $(basename "$0") <command> --help

for command-specific options.

EOF
exit 1
}

usage_build() {
  cat <<EOF

Build the CTA RPMs inside a persistent build container.

The build container is reused across invocations to speed up incremental
development. Use --reset to recreate it from scratch.

Usage:
  $(basename "$0") build [options]

Options:
  -r, --reset                       Recreate the persistent build container
                                    and rebuild the SRPMs.
      --clean-build-dir             Remove build_rpm/.
      --clean-build-dirs            Remove build_rpm/ and build_srpm/.
      --build-generator <generator> CMake generator
                                    ["Unix Makefiles", "Ninja"].
      --cmake-build-type <type>     Release, Debug, RelWithDebInfo,
                                    or MinSizeRel.
      --disable-ccache              Disable ccache.
      --unit-tests                  Run unit tests after building.
      --enable-address-sanitizer    Enable AddressSanitizer.
      --skip-debug-packages         Do not build debuginfo RPMs.
      --force-install               Force SRPM installation.

EOF
exit 1
}

usage_images() {
  cat <<EOF

Build CTA container images from the locally generated RPMs.

Images are loaded into a running local minikube or k3s cluster when one is
detected. Otherwise they remain in the selected container runtime.

Will build a single Docker image per CTA service.
All images are built in parallel.

Usage:
  $(basename "$0") images [options]

Options:
      --enable-debug-image      Build an additional debug image containing
                                debuginfo packages and gdb.
      --skip-image-cleanup      Keep existing CTA images with the selected
                                development tag.

EOF
exit 1
}

usage_deploy() {
  cat <<EOF

Deploy a local CTA development instance.

An existing deployment is replaced only when it was created by CTA tooling.

Usage:
  $(basename "$0") deploy [options]

Options:
      --deploy-namespace <ns>      Kubernetes namespace.
                                   Defaults to dev.
      --catalogue-config <path>    Catalogue values file.
      --scheduler-config <path>    Scheduler values file.
      --cta-config <paths>         Additional CTA Helm values.
                                   Comma-separated list.
      --eos-config <path>          EOS Helm values.
      --eos-image-repository <r>   EOS image repository.
      --eos-image-tag <tag>        EOS image tag.
      --cta-image-tag <tag>        CTA image tag.
      --spawn-options <options>    Additional deployment options.
      --with-dcache                Deploy dCache instead of EOS.
      --local-telemetry            Deploy a local telemetry stack.
      --publish-telemetry          Publish telemetry to the configured backend.

EOF
exit 1
}

usage_test() {
  cat <<EOF

Usage:
  $(basename "$0") test [test] [pytest-options...]

Run a system test.

Note that any options passed AFTER the test name are forwarded to pytest.
If no test is specified, an interactive menu is displayed.

Available tests:
$(printf "  %s\n" "${available_tests[@]}")

Examples:
  $(basename "$0") test
  $(basename "$0") test client
  $(basename "$0") test setup
  $(basename "$0") test verification
  $(basename "$0") test teardown
  $(basename "$0") test client --teardown-first
  $(basename "$0") test client --ff # Resume from the previous failure
  $(basename "$0") test client --lf # Run only the failed tests again
  $(basename "$0") test client -k test_simple_archive_retrieve

Setup, verification, and teardown tests are included by default.
Any additional arguments are passed directly to pytest.
For additional pytest help, navigate to ci/system_tests and run pytest --help

EOF
exit 1
}

# =========================================================================
#  Option parsing
# =========================================================================

unsupported_argument() {
    local message="$1"
    log_error "Invalid option(s) provided:"
    echo
    echo "    ${message}"
    echo
    echo "See --help for additional information."
    exit 1

}

require_command() {
    local option="$1"
    local command="$2"
    shift 2

    local allowed
    for allowed in "$@"; do
        [[ "$allowed" == "$command" ]] && return
    done

    unsupported_argument "${option} is only valid for the following commands: $*"
}

parse_options() {
  local command="$1"
  local -a spawn_options
  shift

  while [[ $# -gt 0 ]]; do
    # Everything after the optional test name belongs to pytest.
    if [[ "$command" == "test" || "$command" == "all" ]]; then
      if [[ -z "${selected_test}" && "$1" != -* ]]; then
        selected_test="$1"

        local test_found=false
        local test
        for test in "${available_tests[@]}"; do
          [[ "$test" == "$selected_test" ]] && test_found=true
        done

        if [[ $test_found == false ]]; then
          log_error "Unknown test: ${selected_test}"
          echo
          print_available_tests
          echo
          echo "Run '$(basename "$0") test' to choose a test interactively."
          exit 1
        fi

        shift
        pytest_args=("$@")
        break
      fi
    fi

    case "$1" in
      -h|--help)
        show_help=true
        ;;

      # =========================================================================
      #  Global options
      # =========================================================================

      --disable-oracle-support)
        oracle_support="false"
        ;;
      --use-public-repos)
        enable_internal_repos=false
        ;;
      --platform)
        platform="$2"
        shift
        ;;
      --scheduler-type)
        scheduler_type="$2"
        shift
        ;;
      --container-runtime)
        container_runtime="$2"
        container_runtime_explicit=true
        shift
        ;;

      # =========================================================================
      #  Build options
      # =========================================================================

      -r|--reset)
        require_command "$1" "$command" build up all
        reset=true
        clean_build_dirs=true
        ;;
      --clean-build-dir)
        require_command "$1" "$command" build up all
        clean_build_dir=true
        ;;
      --clean-build-dirs)
        require_command "$1" "$command" build up all
        clean_build_dirs=true
        ;;
      --disable-ccache)
        require_command "$1" "$command" build up all
        enable_ccache=false
        ;;
      --skip-debug-packages)
        require_command "$1" "$command" build up all
        skip_debug_packages=true
        ;;
      --unit-tests)
        require_command "$1" "$command" build up all
        skip_unit_tests=false
        ;;
      --force-install)
        require_command "$1" "$command" build up all
        force_install=true
        ;;
      --enable-address-sanitizer)
        require_command "$1" "$command" build up all
        enable_address_sanitizer=true
        ;;
      --build-generator)
        require_command "$1" "$command" build up all
        build_generator="$2"
        shift
        ;;
      --cmake-build-type)
        require_command "$1" "$command" build up all
        cmake_build_type="$2"
        shift
        ;;

      # =========================================================================
      #  Image options
      # =========================================================================

      --skip-image-cleanup)
        require_command "$1" "$command" images up all
        image_cleanup=false
        ;;
      --enable-debug-image)
        require_command "$1" "$command" images up all
        enable_debug_image=true
        ;;

      # =========================================================================
      #  Deploy options
      # =========================================================================

      --with-dcache)
        require_command "$1" "$command" deploy up all
        dcache_enabled=true
        eos_enabled=false
        ;;
      --local-telemetry)
        require_command "$1" "$command" deploy up all
        local_telemetry=true
        ;;
      --publish-telemetry)
        require_command "$1" "$command" deploy up all
        publish_telemetry=true
        ;;
      --eos-image-repository)
        require_command "$1" "$command" deploy up all
        eos_image_repository="$2"
        shift
        ;;
      --eos-image-tag)
        require_command "$1" "$command" deploy up all
        eos_image_tag="$2"
        shift
        ;;
      --cta-image-tag)
        require_command "$1" "$command" deploy
        cta_image_tag="$2"
        shift
        ;;
      --catalogue-config)
        require_command "$1" "$command" deploy up all
        catalogue_config="$2"
        shift
        ;;
      --scheduler-config)
        require_command "$1" "$command" deploy up all
        scheduler_config="$2"
        shift
        ;;
      --cta-config)
        require_command "$1" "$command" deploy up all
        cta_config="$2"
        shift
        ;;
      --eos-config)
        require_command "$1" "$command" deploy up all
        eos_config="$2"
        shift
        ;;
      --spawn-options)
        require_command "$1" "$command" deploy up all
        spawn_options=()
        read -ra spawn_options <<< "$2"
        extra_spawn_options+=("${spawn_options[@]}")
        shift
        ;;
      --deploy-namespace)
        require_command "$1" "$command" deploy up all test all
        deploy_namespace="$2"
        shift
        ;;

      *)
        unsupported_argument "Unsupported argument: $1"
        ;;
    esac
    shift
  done

  if [[ "$cmake_build_type" != "Release" && "$cmake_build_type" != "Debug" && "$cmake_build_type" != "RelWithDebInfo" && "$cmake_build_type" != "MinSizeRel" ]]; then
    unsupported_argument "--cmake-build-type is \"$cmake_build_type\" but must be one of [Release, Debug, RelWithDebInfo, or MinSizeRel]."
  fi

  if [[ -n "$container_runtime" && "$container_runtime" != "docker" && "$container_runtime" != "podman" ]]; then
    unsupported_argument "--container-runtime is \"$container_runtime\" but must be one of [docker, podman]."
  fi

  if [[ "$scheduler_type" != "objectstore" ]] && [[ "$scheduler_type" != "pgsched" ]]; then
    unsupported_argument "--scheduler-type is \"$scheduler_type\" but must be one of [objectstore, pgsched]."
  fi

}

validate_container_runtime() {
  command -v "$container_runtime" >/dev/null 2>&1 || return 1
  "$container_runtime" info >/dev/null 2>&1 || return 1
  if [[ $container_runtime == docker ]]; then
    if ! docker buildx version >/dev/null 2>&1; then
      die "Docker Buildx is required to build CTA images. Install the Docker Buildx plugin or use Podman."
    fi
  fi
}

select_container_runtime() {
  local candidate
  if [[ $container_runtime_explicit == true ]]; then
    validate_container_runtime || die "Requested container runtime '$container_runtime' is not installed or usable."
    return
  fi
  for candidate in podman docker; do
    container_runtime="$candidate"
    if validate_container_runtime; then
      log_task "Using container runtime: $container_runtime"
      return
    fi
  done
  die "No usable container runtime found. Install and start Podman or Docker."
}

local_kubernetes_available() {
  command -v minikube >/dev/null 2>&1 || command -v k3s >/dev/null 2>&1
}

ensure_namespace_owned() {
  local owner
  if kubectl get namespace "$deploy_namespace" >/dev/null 2>&1; then
    owner=$(kubectl get namespace "$deploy_namespace" -o 'jsonpath={.metadata.labels.app\.kubernetes\.io/managed-by}')
    [[ $owner == cta-create-instance ]] || \
      die "Refusing to replace namespace '$deploy_namespace': it is not managed by CTA tooling. Delete it manually with: kubectl delete namespace '$deploy_namespace'"
  fi
}

container_exists() {
  local -r container_name="$1"
  if [[ $container_runtime == podman ]]; then
    podman container exists "$container_name"
  else
    docker container inspect "$container_name" >/dev/null 2>&1
  fi
}

container_is_running() {
  local -r container_name="$1"
  local running
  if [[ $container_runtime == podman ]]; then
    running=$(podman container inspect --format '{{.State.Running}}' "$container_name" 2>/dev/null) || return 1
  else
    running=$(docker container inspect --format '{{.State.Running}}' "$container_name" 2>/dev/null) || return 1
  fi
  [[ $running == true ]]
}

# =========================================================================
#  Commands
# =========================================================================

build_cta() {
  cd "$project_root"

  # Constants
  local -r cta_version="5"
  local -r vcs_version="dev"
  local -r xrootd_ssi_version=$(git -C "$project_root/xrootd-ssi-protobuf-interface" describe --tags --exact-match)
  local -r build_image_name="cta-build-image-${platform}"
  local -r build_container_name="cta-build${project_root//\//-}-${platform}"
  local -r mount_basedir="/shared/CTA"
  local -r num_jobs=$(nproc --ignore=2)
  local build_command=("$container_runtime" build)
  [[ $container_runtime == docker ]] && build_command=(docker buildx build --load)

  local rebuild_srpms=false
  local reinstall_srpms=false
  local automatic_clean_build_dirs=false
  local recreate_build_container=false
  local state_is_valid=false
  local build_output_exists=false
  local build_state_json

  # By default we don't rebuild the SRPMs. However, this causes issues when certain flags are switched
  # As such, we create a JSON file in the build directory to track these options
  # If they changed, we rebuild the SRPMs automatically instead of requiring the user to do so manually
  build_state_json=$(jq -cn \
    --argjson schemaVersion "$build_state_schema_version" \
    --arg platform "$platform" \
    --arg schedulerType "$scheduler_type" \
    --argjson oracleSupport "$oracle_support" \
    --arg buildGenerator "$build_generator" \
    --argjson internalRepos "$enable_internal_repos" \
    '{schemaVersion: $schemaVersion, platform: $platform, schedulerType: $schedulerType,
      oracleSupport: $oracleSupport, buildGenerator: $buildGenerator, internalRepos: $internalRepos}')

  [[ -d "${project_root}/build_srpm" || -d "${project_root}/build_rpm" ]] && build_output_exists=true

  if [[ -f "$build_state_file" ]] \
      && jq -e --argjson desired "$build_state_json" \
        '.schemaVersion == $desired.schemaVersion
         and (.platform | type == "string")
         and (.schedulerType | type == "string")
         and (.oracleSupport | type == "boolean")
         and (.buildGenerator | type == "string")
         and (.internalRepos | type == "boolean")' \
        "$build_state_file" >/dev/null 2>&1; then
    state_is_valid=true
  fi

  if [[ $state_is_valid == true ]]; then
    local field old_value new_value
    while IFS=$'\t' read -r field old_value new_value; do
      case "$field" in
        schedulerType) log_warn "Scheduler changed: ${old_value} -> ${new_value}" ;;
        oracleSupport) log_warn "Oracle support changed: ${old_value} -> ${new_value}" ;;
        buildGenerator) log_warn "Build generator changed: ${old_value} -> ${new_value}" ;;
        platform) log_warn "Build platform changed: ${old_value} -> ${new_value}" ;;
        internalRepos)
          log_warn "Repository mode changed: internal repositories ${old_value} -> ${new_value}"
          recreate_build_container=true
          ;;
      esac
      [[ "$field" != "internalRepos" ]] && automatic_clean_build_dirs=true
    done < <(jq -r --argjson desired "$build_state_json" '
      ["schedulerType", "oracleSupport", "buildGenerator", "platform", "internalRepos"][] as $field
      | select(.[$field] != $desired[$field])
      | [$field, (.[$field] | tostring), ($desired[$field] | tostring)]
      | @tsv' "$build_state_file")

    if [[ ! -d "${project_root}/build_srpm" ]]; then
      log_warn "The recorded SRPM build directory is missing."
      automatic_clean_build_dirs=true
    fi
  else
    rebuild_srpms=true
    reinstall_srpms=true
    if [[ $build_output_exists == true ]]; then
      if [[ -f "$build_state_file" ]]; then
        log_warn "Build state is invalid or from an unsupported version."
      else
        log_warn "Existing build output has no recorded build state."
      fi
      automatic_clean_build_dirs=true
    fi
  fi

  if [[ $automatic_clean_build_dirs == true ]]; then
    log_warn "Automatically cleaning build_srpm/ and build_rpm/."
    clean_build_dirs=true
    rebuild_srpms=true
    reinstall_srpms=true
  fi

  if [[ $recreate_build_container == true ]]; then
    log_warn "Automatically recreating the build container to refresh repository configuration."
    reset=true
    clean_build_dirs=true
    rebuild_srpms=true
    reinstall_srpms=true
  fi

  if [[ $clean_build_dirs == true ]]; then
    rebuild_srpms=true
    reinstall_srpms=true
  fi

  # Remove the project-specific container if reset is requested.
  if [[ "${reset}" = true ]]; then
    if container_exists "$build_container_name"; then
      log_task "Removing build container ${build_container_name}..."
      ${container_runtime} rm -f "${build_container_name}" >/dev/null
    fi
    if container_exists "$build_container_name"; then
      die "Failed to remove build container '${build_container_name}'. Refusing to rebuild while the name is still in use."
    fi
  fi

  # Reuse the exact project-specific container, including a stopped one.
  if container_exists "$build_container_name"; then
    if container_is_running "$build_container_name"; then
      log_task "Reusing running build container: ${build_container_name}"
    else
      log_task "Starting existing stopped build container ${build_container_name}..."
      ${container_runtime} start "$build_container_name" >/dev/null \
        || die "Failed to start existing build container '${build_container_name}'."
    fi
  else
    print_header "SETTING UP BUILD CONTAINER"
    build_container_restarted=true
    log_task "Building the build container image..."
    "${build_command[@]}" --no-cache -t "${build_image_name}" -f ci/docker/cta/"${platform}"/build.Dockerfile .
    log_task "Starting build container ${build_container_name}..."
    local run_output
    if ! run_output=$(${container_runtime} run -dit --rm --name "${build_container_name}" \
      -v "${project_root}:${mount_basedir}:z" \
      "${build_image_name}" \
      /bin/bash 2>&1); then
      if [[ $run_output =~ [Nn]ame.*already.*in.*use || $run_output =~ name.*is.*in.*use ]]; then
        die "Container runtime reported that '${build_container_name}' does not exist, but its name is still reserved. Inspect it with '${container_runtime} container inspect ${build_container_name}' and remove the stale container or storage record before retrying. Runtime error: ${run_output}"
      fi
      die "Failed to create build container '${build_container_name}': ${run_output}"
    fi

  fi

  if [[ $build_container_restarted == true || $rebuild_srpms == true ]]; then
    print_header "BUILDING SRPMS"
    local build_srpm_flags=()
    [[ $clean_build_dirs == true ]] && build_srpm_flags+=(--clean-build-dir)

    ${container_runtime} exec "${build_container_name}" \
      .${mount_basedir}/ci/build/build_srpm.sh \
      --build-dir ${mount_basedir}/build_srpm \
      --build-generator "${build_generator}" \
      --create-build-dir \
      --cta-version "${cta_version}" \
      --vcs-version "${vcs_version}" \
      --scheduler-type "${scheduler_type}" \
      --oracle-support "${oracle_support}" \
      --cmake-build-type "${cmake_build_type}" \
      --jobs "${num_jobs}" \
      "${build_srpm_flags[@]}"
  fi

  log_task "Compiling CTA from the source directory..."

  local build_rpm_flags=()

  if [[ $build_container_restarted == true || $reinstall_srpms == true || $force_install == true ]]; then
    [[ $reinstall_srpms == true ]] && log_task "Refreshing build dependencies from the regenerated SRPMs..."
    build_rpm_flags+=(--install-srpms)
  fi

  [[ $skip_unit_tests == true ]] && build_rpm_flags+=(--skip-unit-tests)
  [[ $clean_build_dir == true || $clean_build_dirs == true ]] && build_rpm_flags+=(--clean-build-dir)
  [[ $skip_debug_packages == true ]] && build_rpm_flags+=(--skip-debug-packages)
  [[ $enable_ccache == true ]] && build_rpm_flags+=(--enable-ccache)
  [[ $enable_internal_repos == true ]] && build_rpm_flags+=(--enable-internal-repos)
  [[ $enable_address_sanitizer == true ]] && build_rpm_flags+=(--enable-address-sanitizer)

  print_header "BUILDING RPMS"
  ${container_runtime} exec "${build_container_name}" \
    .${mount_basedir}/ci/build/build_rpm.sh \
    --build-dir ${mount_basedir}/build_rpm \
    --build-generator "${build_generator}" \
    --create-build-dir \
    --srpm-dir ${mount_basedir}/build_srpm/RPM/SRPMS \
    --cta-version ${cta_version} \
    --vcs-version ${vcs_version} \
    --xrootd-ssi-version "${xrootd_ssi_version}" \
    --scheduler-type "${scheduler_type}" \
    --oracle-support ${oracle_support} \
    --cmake-build-type "${cmake_build_type}" \
    --jobs "${num_jobs}" \
    --platform "${platform}" \
    "${build_rpm_flags[@]}"

  printf '%s\n' "$build_state_json" >"$build_state_file"

}

images_cta() {
  # Constants
  local -r rpm_src="build_rpm/RPM/RPMS/x86_64" # note relative to project root

  print_header "BUILDING CONTAINER IMAGES"
  # Cleanup
  if [[ ${image_cleanup} = true ]]; then
    log_task "Cleaning up CTA images tagged ${cta_image_tag}..."
    local target
    for target in cta-taped cta-maintd cta-rmcd cta-frontend cta-tools cta-debug; do
      ${container_runtime} image rm "cta/ctageneric/${target}:${cta_image_tag}" >/dev/null 2>&1 || true
    done
  else
    log_warn "Skipping cleanup of unused container images."
  fi

  # Build
  log_task "Building container images from ${rpm_src}..."
  local extra_image_build_options=()
  local load_into_k8s=false
  [[ $enable_internal_repos == true ]] && extra_image_build_options+=(--enable-internal-repos)
  [[ $enable_debug_image == true ]] && extra_image_build_options+=(--enable-debug-image)
  if local_kubernetes_available; then
    extra_image_build_options+=(--load-into-k8s)
    load_into_k8s=true
  fi
  cd "${project_root}"
  ./ci/build/build_images.sh \
    --tag "${cta_image_tag}" \
    --rpm-src "${rpm_src}" \
    --container-runtime "${container_runtime}" \
    "${extra_image_build_options[@]}"
  if [[ $load_into_k8s == false ]]; then
    log_warn "Kubernetes image loading skipped: neither minikube nor k3s is installed."
  fi
}

deploy_cta() {
  ensure_namespace_owned
  print_header "DELETING OLD CTA DEPLOYMENTS"

  cd "${project_root}/ci/orchestration"
  # By default we discard the logs from deletion as this is not very useful during development and pollutes the dev machine
  ./delete_instance.sh -n "${deploy_namespace}" --discard-logs --keep-pvs
  print_header "DEPLOYING CTA"
  [[ -n $eos_image_repository ]] && extra_spawn_options+=(--eos-image-repository "$eos_image_repository")
  [[ -n $eos_image_tag ]] && extra_spawn_options+=(--eos-image-tag "$eos_image_tag")
  [[ -n $eos_config ]] && extra_spawn_options+=(--eos-config "$eos_config")
  [[ -n $cta_config ]] && extra_spawn_options+=(--cta-config "$cta_config")
  [[ $local_telemetry == true ]] && extra_spawn_options+=(--local-telemetry)
  [[ $publish_telemetry == true ]] && extra_spawn_options+=(--publish-telemetry)

  # Assign default config based on scheduler type if not already set
  if [[ -z $scheduler_config ]]; then
    [[ $scheduler_type == "pgsched" ]] && \
      scheduler_config="presets/dev-scheduler-postgres-values.yaml" || \
      scheduler_config="presets/dev-scheduler-vfs-values.yaml"
  fi
  ./create_instance.sh --namespace "${deploy_namespace}" \
    --cta-image-registry localhost \
    --cta-image-tag "${cta_image_tag}" \
    --catalogue-config "${catalogue_config}" \
    --scheduler-config "${scheduler_config}" \
    --eos-enabled ${eos_enabled} \
    --dcache-enabled ${dcache_enabled} \
    --reset-catalogue \
    --reset-scheduler \
    "${extra_spawn_options[@]}"
}

test_cta() {
  [[ -x "$venv_dir/bin/python" && -x "$venv_dir/bin/pytest" ]] || \
    die "CTA system-test environment is missing. Run '$(basename "$0") install' first."
  print_header "RUNNING TESTS"

  source "$venv_dir/bin/activate" || (log_error "Failed to activate the Python virtual environment. Run \"$(basename "$0") install\" to create it." && exit 1)

  if [[ -z "${selected_test}" ]]; then
    PS3="Select test: "
    select selected_test in "${available_tests[@]}"; do
      [[ -n "${selected_test}" ]] && break
      log_error "Invalid selection."
    done
  fi

  cd "${project_root}/ci/system_tests"

  log_task "Running system test ${selected_test}..."
  local test_path
  local lifecycle_options=()
  case "${selected_test}" in
    setup|verification|teardown)
      test_path="tests/${selected_test}/"
      ;;
    *)
      test_path="tests/${selected_test}_test.py"
      lifecycle_options=(--setup --verification --teardown)
      ;;
  esac

  pytest \
    "${test_path}" \
    --namespace "${deploy_namespace}" \
    "${lifecycle_options[@]}" \
    "${pytest_args[@]}"

  deactivate
}

run_timed_stage() {
  local -r stage_name="$1"
  local -r stage_function="$2"
  local -r start_time=$SECONDS

  "$stage_function"

  stage_names+=("$stage_name")
  stage_durations+=("$((SECONDS - start_time))")
}

print_stage_summary() {
  echo
  echo "$program_name stage durations:"

  local i total_duration=0
  for i in "${!stage_names[@]}"; do
    printf "  %-7s %d seconds\n" "${stage_names[$i]}:" "${stage_durations[$i]}"
    total_duration=$((total_duration + stage_durations[i]))
  done
  printf "  %-7s %d seconds\n" "Total:" "$total_duration"
}

up_cta() {
  run_timed_stage "Build" build_cta
  run_timed_stage "Images" images_cta
  run_timed_stage "Deploy" deploy_cta
  print_stage_summary
}

all_cta() {
  run_timed_stage "Build" build_cta
  run_timed_stage "Images" images_cta
  run_timed_stage "Deploy" deploy_cta
  test_cta
  print_stage_summary
}

install_cta_dev() {
  local -r bin_dir="$HOME/.local/bin"
  local -r link_path="$bin_dir/$program_name"
  local -r script_path="$(realpath "$0")"
  local -r requirements_path="${project_root}/ci/system_tests/requirements.txt"
  local -r completion_src="${project_root}/ci/${program_name}.bash-completion"
  local -r completion_dir="$HOME/.local/share/bash-completion/completions"
  local -r completion_dst="${completion_dir}/${program_name}"

  echo "This will:"
  echo "  - Install the $program_name command symlink to $link_path"
  echo "  - Install Bash completion to $completion_dst"
  echo "  - Create a Python virtual environment in $venv_dir"
  echo "  - Install dependencies from $requirements_path"
  read -r -p "Continue? [y/N] " confirm
  [[ "$confirm" =~ ^[Yy]$ ]] || return 1

  log_task "Installing the ${program_name} command..."
  mkdir -p "$bin_dir"
  ln -sf "$script_path" "$link_path"

  mkdir -p "$completion_dir"
  install -m 644 "$completion_src" "$completion_dst"

  if command -v uv >/dev/null 2>&1; then
    uv venv "$venv_dir"
    uv pip install --python "$venv_dir/bin/python" -r "$requirements_path"
  else
    python3 -m venv "$venv_dir"
    "$venv_dir/bin/pip" install -r "$requirements_path"
  fi

  log_success "Installed ${program_name}."
  log_warn "Restart your shell to use ${program_name} and its Bash completion."
}

# =========================================================================
#  Main
# =========================================================================

main() {
  if [[ $# -eq 0 ]]; then
    usage
    exit 1
  fi

  local -r command="$1"
  shift

  if [[ "$command" == "help" || "$command" == "-h" || "$command" == "--help" ]]; then
    usage
    exit 0
  fi

  load_cta_dev_env
  parse_options "$command" "$@"
  select_container_runtime

  case "$command" in
    build)
      [[ $show_help == true ]] && usage_build
      build_cta
      ;;
    images)
      [[ $show_help == true ]] && usage_images
      images_cta
      ;;
    deploy)
      [[ $show_help == true ]] && usage_deploy
      deploy_cta
      ;;
    test)
      [[ $show_help == true ]] && usage_test
      test_cta
      ;;
    up)
      [[ $show_help == true ]] && usage
      up_cta
      ;;
    all)
      [[ $show_help == true ]] && usage
      all_cta
      ;;
    install)
      [[ $show_help == true ]] && usage
      install_cta_dev
      ;;
    *)
      unsupported_argument "Unknown command: $command"
      ;;
  esac
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
