#!/bin/bash

# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -eo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/utils/log_wrapper.sh"

# Help message
usage() {
echo
  echo "Orchestrates the continuous integration lifecycle for CTA."
  echo "The script executes a multi-stage delivery pipeline sequentially:"
  echo
  echo "  [1. BUILD RPMS]             [2. BUILD IMAGES]             [3. DEPLOY SETUP]"
  echo "  +--------------------+      +----------------------+      +------------------------+"
  echo "  | Compiles source    |      | Takes the built RPMs |      | Tears down old env and |"
  echo "  | inside a persistent| ---> | and uses them to     | ---> | spawns a k8s instance  |"
  echo "  | build container    |      | build docker images  |      | with the built images  |"
  echo "  +--------------------+      +----------------------+      +------------------------+"
  echo
  echo "The build container persists between runs (unless --reset is specified) to ensure"
  echo "incremental compilation speeds rather than compiling from scratch every time."
  echo
  echo "Important prerequisite: to run system tests, your machine must have cluster access"
  echo "to a Kubernetes setup containing worker nodes with 'mhvtl' (virtual tape library) installed."
  echo
  echo "Usage: $0 [options]"
  echo
  echo "General Options:"
  echo "  -h, --help                            Shows help output."
  echo "  -r, --reset                           Shut down the build container, clear directories, and start fresh."
  echo "  -c, --container-runtime <runtime>     Container runtime to use [docker, podman]. Defaults to podman."
  echo "      --platform <platform>             Which platform to build for. Defaults to project.json."
  echo "      --scheduler-type <type>           The scheduler backend type [objectstore, pgsched]."
  echo "      --use-public-repos                Use public yum repos instead of CERN internal network repos."
  echo
  echo "Build Configuration:"
  echo "      --build-generator <generator>     Specifies the build generator for cmake [\"Unix Makefiles\", \"Ninja\"]."
  echo "      --cmake-build-type <type>         Specifies cmake build type [Release, Debug, RelWithDebInfo, MinSizeRel]."
  echo "      --clean-build-dir                 Empties the RPM build directory (build_rpm/)."
  echo "      --clean-build-dirs                Empties both the SRPM (build_srpm/) and RPM (build_rpm/) directories."
  echo "      --disable-oracle-support          Disables support for Oracle."
  echo "      --disable-ccache                  Disables ccache for building the RPMs."
  echo "      --enable-address-sanitizer        Compile with address sanitizer enabled."
  echo "      --force-install                   Adds --install-srpms flag to build_rpm step even if not reset."
  echo "      --skip-build                      Skips the build step completely."
  echo "      --skip-cmake                      Skips the cmake step of the build_rpm stage."
  echo "      --skip-debug-packages             Skips the building of the debug RPM packages."
  echo "      --enable-unit-tests               Runs the unit tests after the RPMs have been built."
  echo
  echo "Image Building & Packaging:"
  echo "      --skip-image-build                Skips building of the Docker images and loading them into the Kubernetes cluster."
  echo "      --skip-image-cleanup              Skips cleanup of ctageneric images in runtime and cluster before deploy."
  echo "      --enable-debug-image             Builds an additional image containing all CTA RPMs, their debuginfo RPMs and gdb."
  echo "      --image-build-options <options>   Additional options passed verbatim to the build_images.sh script."
  echo
  echo "Deployment & Orchestration Configuration:"
  echo "      --skip-deploy                     Skips the deployment step completely."
  echo "      --deploy-namespace <namespace>    Deploy the CTA instance in a given namespace. Defaults to dev."
  echo "      --upgrade-cta                     Upgrades the existing CTA instance instead of spawning from scratch."
  echo "      --upgrade-eos                     Upgrades the existing EOS instance instead of spawning from scratch."
  echo "      --spawn-options <options>         Additional options passed verbatim to create/upgrade instance scripts."
  echo "      --no-setup                        Skip setup scripts in create_instance.sh (required for Python tests)."
  echo "      --scheduler-config <path>         Path to scheduler values yaml file."
  echo "      --catalogue-config <path>         Path to catalogue values yaml file."
  echo "      --cta-config <paths>              Custom Values yaml file(s) for CTA Helm chart (comma-separated)."
  echo "      --eos-config <path>               Custom Values yaml file to pass to the EOS Helm chart."
  echo "      --eos-image-repository <repo>     Image repository URL/namespace path to use for spawning EOS."
  echo "      --eos-image-tag <tag>             Image tag to use for spawning EOS."
  echo "      --cta-image-tag <tag>             Image tag to use for spawning CTA. Will skip both build stages."
  echo "      --disable-eos                     Skips spawning EOS in the system tests."
  echo "      --enable-dcache                   Spawns dCache in the system tests."
  echo
  echo "Telemetry & Observability:"
  echo "      --local-telemetry                 Spawns a local collector and Prometheus backends for metrics."
  echo "      --publish-telemetry               Publishes telemetry to a pre-configured central backend."
  echo
  exit 1
}

build_deploy() {

  local project_root
  project_root=$(git rev-parse --show-toplevel)

  # Defaults
  local num_jobs
  num_jobs=$(nproc --ignore=2)
  local restarted=false
  local deploy_namespace="dev"
  local cta_version="5"
  local vcs_version="dev"
  local xrootd_ssi_version
  xrootd_ssi_version=$(cd "$project_root/xrootd-ssi-protobuf-interface" && git describe --tags --exact-match)

  # Input flag values
  local clean_build_dir=false
  local clean_build_dirs=false
  local force_install=false
  local reset=false
  local skip_build=false
  local skip_deploy=false
  local skip_cmake=false
  local skip_unit_tests=true
  local skip_debug_packages=false
  local skip_image_build=false
  local local_telemetry=false
  local publish_telemetry=false
  local no_setup=false
  local build_generator="Ninja"
  local cmake_build_type
  cmake_build_type=$(jq -r .dev.defaultBuildType "${project_root}/project.json")
  local scheduler_type="objectstore"
  local oracle_support="TRUE"
  local enable_ccache=true
  local upgrade_cta=false
  local upgrade_eos=false
  local image_cleanup=true
  local extra_spawn_options=""
  local extra_image_build_options=""
  local catalogue_config="presets/dev-catalogue-postgres-values.yaml"
  local scheduler_config=""
  local eos_image_repository=""
  local eos_image_tag=""
  local cta_image_tag=""
  local container_runtime="podman"
  local platform
  platform=$(jq -r .dev.defaultPlatform "${project_root}/project.json")
  local enable_internal_repos=true
  local enable_debug_image=false
  local eos_enabled=true
  local dcache_enabled=false
  local enable_address_sanitizer=false
  local cta_config=""
  local eos_config=""


  # Parse command line arguments
  while [[ "$#" -gt 0 ]]; do
    # Helper to validate options requiring an argument
    if [[ "$1" =~ ^--(eos-image-repository|eos-image-tag|cta-image-tag|platform|build-generator|cmake-build-type|container-runtime|scheduler-type|catalogue-config|scheduler-config|tapeservers-config|cta-config|eos-config|spawn-options|image-build-options|deploy-namespace)$ || "$1" == "-c" ]]; then
      if [[ -z "$2" || "$2" =~ ^- ]]; then
        error_usage "$1 requires an argument"
      fi
    fi

    case "$1" in
      -h | --help)                  usage ;;
      -r | --reset)                 reset=true; clean_build_dirs=true ;;
      --clean-build-dir)            clean_build_dir=true ;;
      --clean-build-dirs)           clean_build_dirs=true ;;
      --disable-oracle-support)     oracle_support="FALSE" ;;
      --disable-ccache)             enable_ccache=false ;;
      --skip-build)                 skip_build=true ;;
      --skip-deploy)                skip_deploy=true ;;
      --skip-cmake)                 skip_cmake=true ;;
      --skip-debug-packages)        skip_debug_packages=true ;;
      --skip-image-build)           skip_image_build=true ;;
      --skip-image-cleanup)         image_cleanup=false ;;
      --enable-unit-tests)          skip_unit_tests=false ;;
      --force-install)              force_install=true ;;
      --enable-dcache)              dcache_enabled=true ;;
      --disable-eos)                eos_enabled=false ;;
      --upgrade-cta)                upgrade_cta=true ;;
      --upgrade-eos)                upgrade_eos=true ;;
      --use-public-repos)           enable_internal_repos=false ;;
      --enable-debug-image)         enable_debug_image=false ;;
      --local-telemetry)            local_telemetry=true ;;
      --publish-telemetry)          publish_telemetry=true ;;
      --no-setup)                   no_setup=true ;;
      --enable-address-sanitizer)   enable_address_sanitizer=true ;;
      --eos-image-repository)       eos_image_repository="$2"; shift ;;
      --eos-image-tag)              eos_image_tag="$2"; shift ;;
      --cta-image-tag)              cta_image_tag="$2"; shift ;;
      --build-generator)            build_generator="$2"; shift ;;
      --scheduler-type)             scheduler_type="$2"; shift ;;
      --catalogue-config)           catalogue_config="$2"; shift ;;
      --scheduler-config)           scheduler_config="$2"; shift ;;
      --cta-config)                 cta_config="$2"; shift ;;
      --eos-config)                 eos_config="$2"; shift ;;
      --spawn-options)              extra_spawn_options+=" $2"; shift ;;
      --image-build-options)        extra_image_build_options+=" $2"; shift ;;
      --deploy-namespace)           deploy_namespace="$2"; shift ;;
      --platform)
        if [[ "$(jq --arg platform "$2" '.platforms | has($platform)' "$project_root/project.json")" != "true" ]]; then
            error_usage "platform $2 not supported. Please check the project.json for supported platforms."
        fi
        platform="$2"; shift
        ;;
      --cmake-build-type)
        if [[ "$2" != "Release" && "$2" != "Debug" && "$2" != "RelWithDebInfo" && "$2" != "MinSizeRel" ]]; then
          die "--cmake-build-type is \"$2\" but must be one of [Release, Debug, RelWithDebInfo, or MinSizeRel]."
        fi
        cmake_build_type="$2"; shift
        ;;
      -c | --container-runtime)
        if [[ "$2" != "docker" && "$2" != "podman" ]]; then
          die "-c | --container-runtime is \"$2\" but must be one of [docker, podman]."
        fi
        container_runtime="$2"; shift
        ;;
      *)
        die_usage "Unsupported argument: $1"
        ;;
    esac
    shift
  done

  # navigate to root project directory
  cd "${project_root}"

  if [[ -n "$cta_image_tag" ]]; then
    skip_build=true
    skip_image_build=true
    image_tag=$cta_image_tag
  fi

  # =========================================================================
  # Build binaries/RPMs
  # =========================================================================
  if [[ "${skip_build}" = false ]]; then
    build_image_name="cta-build-image-${platform}"
    build_container_name="cta-build${project_root//\//-}-${platform}"
    # Stop and remove existing container if reset is requested
    echo "Total CTA build containers found: $(${container_runtime} ps | grep -c cta-build)"
    if [[ "${reset}" = true ]]; then
      echo "Shutting down existing build container..."
      ${container_runtime} rm -f "${build_container_name}" >/dev/null 2>&1 || true
      ${container_runtime} rmi "${build_image_name}" > /dev/null 2>&1 || true
    fi

    # Start container if not already running
    if ${container_runtime} ps -a --format '{{.Names}}' | grep -wq "${build_container_name}"; then
      echo "Found existing build container: ${build_container_name}"
    else
      print_header "SETTING UP BUILD CONTAINER"
      restarted=true
      echo "Rebuilding build container image"
      ${container_runtime} build --no-cache -t "${build_image_name}" -f ci/docker/cta/"${platform}"/build.Dockerfile .
      echo "Starting new build container: ${build_container_name}"
      ${container_runtime} run -dit --rm --name "${build_container_name}" \
        -v "${project_root}:/shared/CTA:z" \
        "${build_image_name}" \
        /bin/bash

      print_header "BUILDING SRPMS"
      build_srpm_flags=""
      if [[ "${clean_build_dirs}" = true ]]; then
        build_srpm_flags+=" --clean-build-dir"
      fi

      # shellcheck disable=SC2086
      ${container_runtime} exec -it "${build_container_name}" \
        ./shared/CTA/ci/build/build_srpm.sh \
        --build-dir /shared/CTA/build_srpm \
        --build-generator "${build_generator}" \
        --create-build-dir \
        --cta-version "${cta_version}" \
        --vcs-version "${vcs_version}" \
        --scheduler-type "${scheduler_type}" \
        --oracle-support "${oracle_support}" \
        --cmake-build-type "${cmake_build_type}" \
        --jobs "${num_jobs}" \
        ${build_srpm_flags}
    fi

    echo "Compiling the CTA project from source directory"

    local build_rpm_flags=""

    # Only install srpms if it is the first time running this or if the install is forced
    if [[ ${restarted} = true ]] || [[ ${force_install} = true ]]; then
      build_rpm_flags+=" --install-srpms"
    elif [[ ${skip_cmake} = true ]]; then
      # It should only be possible to skip cmake if the pod was not restarted
      build_rpm_flags+=" --skip-cmake"
    fi
    if [[ ${skip_unit_tests} = true ]]; then
      build_rpm_flags+=" --skip-unit-tests"
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

    if [[ ${enable_internal_repos} = true ]]; then
      build_rpm_flags+=" --enable-internal-repos"
    fi

    if [[ ${enable_address_sanitizer} = true ]]; then
      build_rpm_flags+=" --enable-address-sanitizer"
    fi

    print_header "BUILDING RPMS"
    # shellcheck disable=SC2086
    ${container_runtime} exec -it "${build_container_name}" \
      ./shared/CTA/ci/build/build_rpm.sh \
      --build-dir /shared/CTA/build_rpm \
      --build-generator "${build_generator}" \
      --create-build-dir \
      --srpm-dir /shared/CTA/build_srpm/RPM/SRPMS \
      --cta-version ${cta_version} \
      --vcs-version ${vcs_version} \
      --xrootd-ssi-version "${xrootd_ssi_version}" \
      --scheduler-type "${scheduler_type}" \
      --oracle-support ${oracle_support} \
      --cmake-build-type "${cmake_build_type}" \
      --jobs "${num_jobs}" \
      --platform "${platform}" \
      ${build_rpm_flags}

    echo "Build successful"
  fi


  # =========================================================================
  # Build image
  # =========================================================================
  build_iteration_file=/tmp/.build_iteration
  if [[ "$skip_image_build" == "false" ]]; then
    print_header "BUILDING CONTAINER IMAGE"
    # Cleanup
    if [[ ${image_cleanup} = true ]]; then
      echo "Cleaning up unused images..."
      ${container_runtime} image prune -f
      if command -v minikube >/dev/null 2>&1; then
        minikube ssh -- "${container_runtime} image prune -f"
      fi
      if command -v k3s >/dev/null 2>&1; then
        sudo /usr/local/bin/k3s crictl rmi --prune || true
      fi
    fi
    # Determine tag
    if [[ "$upgrade_cta" == "false" ]]; then
      # Start with the tag dev-0
      local current_build_id=0
      image_tag="dev-$current_build_id"
      touch $build_iteration_file
      echo $current_build_id >$build_iteration_file
    else
      # This continuously increments the image tag from previous upgrades
      if [[ ! -f "$build_iteration_file" ]]; then
        die "Failed to find $build_iteration_file to retrieve build iteration."
      fi
      local current_build_id
      current_build_id=$(cat "$build_iteration_file")
      new_build_id=$((current_build_id + 1))
      image_tag="dev-$new_build_id"
      echo $new_build_id >$build_iteration_file
    fi
    # Build
    local rpm_src="build_rpm/RPM/RPMS/x86_64"
    echo "Building image from ${rpm_src}"
    if [[ ${enable_internal_repos} = true ]]; then extra_image_build_options+=" --enable-internal-repos"; fi
    if [[ ${enable_debug_image} = true ]]; then extra_image_build_options+=" --enable-debug-image"; fi
    # shellcheck disable=SC2086
    ./ci/build/build_images.sh \
      --tag ${image_tag} \
      --rpm-src "${rpm_src}" \
      --container-runtime "${container_runtime}" \
      --load-into-k8s \
      ${extra_image_build_options}
  else
    if [[ ! -f "$build_iteration_file" ]]; then
      die "Failed to find $build_iteration_file to retrieve build iteration. Unable to identify which image to spawn/upgrade the instance with."
    fi
    # If we are not building a new image, use the latest one
    image_tag=dev-$(cat "$build_iteration_file")
  fi

  # =========================================================================
  # Deploy CTA instance
  # =========================================================================
  if [[ ${skip_deploy} = false ]]; then
    if [[ "$upgrade_cta" = true ]]; then
      print_header "UPGRADING CTA INSTANCE"
      local upgrade_options=""
      if [[ "$skip_image_build" == "false" ]]; then
        upgrade_options+=" --cta-image-registry localhost --cta-image-tag ${image_tag}"
      fi
       # shellcheck disable=SC2086
      (cd ci/orchestration && ./upgrade_cta_instance.sh --namespace "${deploy_namespace}" ${upgrade_options} ${extra_spawn_options})
    elif [[ "$upgrade_eos" = true ]]; then
      print_header "UPGRADING EOS INSTANCE"
      (cd ci/orchestration && ./deploy_eos.sh --namespace "${deploy_namespace}" --eos-image-repository "${eos_image_repository}" --eos-image-tag "${eos_image_tag}")
    else
      print_header "DELETING OLD CTA INSTANCES"
      # By default we discard the logs from deletion as this is not very useful during development and pollutes the dev machine
      ./ci/orchestration/delete_instance.sh -n "${deploy_namespace}" --discard-logs
      print_header "DEPLOYING CTA INSTANCE"
      if [[ -n "${eos_image_repository}" ]]; then extra_spawn_options+=" --eos-image-repository ${eos_image_repository}"; fi
      if [[ -n "${eos_image_tag}" ]]; then extra_spawn_options+=" --eos-image-tag ${eos_image_tag}"; fi
      if [[ -n "${eos_config}" ]]; then extra_spawn_options+=" --eos-config ${eos_config}"; fi
      if [[ -n "${cta_config}" ]]; then extra_spawn_options+=" --cta-config ${cta_config}"; fi
      if [[ "${local_telemetry}" = true ]]; then extra_spawn_options+=" --local-telemetry"; fi
      if [[ "${publish_telemetry}" = true ]]; then extra_spawn_options+=" --publish-telemetry"; fi
      if [[ "${no_setup}" = true ]]; then extra_spawn_options+=" --no-setup"; fi

      if [[ -z "${scheduler_config}" ]]; then
        if [[ "${scheduler_type}" == "pgsched" ]]; then
          scheduler_config="presets/dev-scheduler-postgres-values.yaml"
        else
          scheduler_config="presets/dev-scheduler-vfs-values.yaml"
        fi
      fi

      echo "Deploying CTA instance"
      cd ci/orchestration
       # shellcheck disable=SC2086
      ./create_instance.sh --namespace "${deploy_namespace}" \
        --cta-image-registry localhost \
        --cta-image-tag "${image_tag}" \
        --catalogue-config "${catalogue_config}" \
        --scheduler-config "${scheduler_config}" \
        --reset-catalogue \
        --reset-scheduler \
        --eos-enabled ${eos_enabled} \
        --dcache-enabled ${dcache_enabled} \
        ${extra_spawn_options}
    fi
  fi
  print_header "DONE"
}

build_deploy "$@"
