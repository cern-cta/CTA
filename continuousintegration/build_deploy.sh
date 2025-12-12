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
source "$(dirname "${BASH_SOURCE[0]}")/utils/log_wrapper.sh"

# Help message
usage() {
  echo
  echo "Performs the build of CTA through a dedicated build container."
  echo "The container persists between runs of this script (unless the --reset flag is specified), which ensures that the build does not need to happen from scratch."
  echo "It is also able to deploy the built rpms on a kubernetes cluster for a basic testing setup."
  echo ""
  echo "Important prerequisite: to run the tests, your machine will need to have access to a kubernetes cluster setup with nodes that have mhvtl installed."
  echo ""
  echo "Usage: $0 [options]"
  echo
  echo "options:"
  echo "  -h, --help:                           Shows help output."
  echo "  -r, --reset:                          Shut down the build container and start a new one to ensure a fresh build. Also cleans the build directories."
  echo "  -c, --container-runtime <runtime>     The container runtime to use for the build container. Defaults to podman."
  echo "  -b, --build-image <image>             Base image to use for the build container. Defaults to the image specified in project.json."
  echo "      --build-generator <generator>:    Specifies the build generator for cmake. Supported: [\"Unix Makefiles\", \"Ninja\"]."
  echo "      --clean-build-dir:                Empties the RPM build directory (build_rpm/ by default), ensuring a fresh build from scratch."
  echo "      --clean-build-dirs:               Empties both the SRPM and RPM build directories (build_srpm/ and build_rpm/ by default), ensuring a fresh build from scratch."
  echo "      --cmake-build-type <type>:        Specifies the build type for cmake. Must be one of [Release, Debug, RelWithDebInfo, or MinSizeRel]."
  echo "      --disable-oracle-support:         Disables support for oracle."
  echo "      --disable-ccache:                 Disables ccache for the building of the rpms."
  echo "      --enable-address-sanitizer:       Compile with address sanitizer enabled."
  echo "      --force-install:                  Adds the --install-srpm flag to the build_rpm step, regardless of whether the container was reset or not."
  echo "      --skip-build:                     Skips the build step."
  echo "      --skip-deploy:                    Skips the deploy step."
  echo "      --skip-cmake:                     Skips the cmake step of the build_rpm stage during the build process."
  echo "      --skip-debug-packages             Skips the building of the debug RPM packages."
  echo "      --skip-unit-tests:                Skips the unit tests. Speeds up the build time by not running the unit tests."
  echo "      --skip-image-reload:              Skips the step where the image is reloaded into Kubernetes cluster. This allows easy redeployment with the image that is already loaded."
  echo "      --skip-image-cleanup:             Skip the cleanup of the ctageneric images in both the container runtime and the cluster before deploying a new instance."
  echo "      --scheduler-type <type>:          The scheduler type. Must be one of [objectstore, pgsched]."
  echo "      --spawn-options <options>:        Additional options to pass for the deployment. These are passed verbatim to the create/upgrade instance scripts."
  echo "      --image-build-options <options>:  Additional options to pass for the image building. These are passed verbatim to the build_image.sh script."
  echo "      --scheduler-config <path>:        Path to the yaml file containing the type and credentials to configure the Scheduler. Defaults to: presets/dev-scheduler-vfs-values.yaml"
  echo "      --catalogue-config <path>:        Path to the yaml file containing the type and credentials to configure the Catalogue. Defaults to: presets/dev-catalogue-postgres-values.yaml"
  echo "      --tapeservers-config <path>:      Path to the yaml file containing the tapeservers config. If not provided, this will be auto-generated."
  echo "      --upgrade-cta:                    Upgrades the existing CTA instance with a new image instead of spawning an instance from scratch."
  echo "      --upgrade-eos:                    Upgrades the existing EOS instance with a new image instead of spawning an instance from scratch."
  echo "      --eos-image-tag <tag>:            Image to use for spawning EOS. If not provided, will default to the image specified in the create_instance script."
  echo "      --cta-config <path>:              Custom Values file to pass to the CTA Helm chart. Defaults to: presets/dev-cta-xrd-values.yaml"
  echo "      --eos-config <path>:              Custom Values file to pass to the EOS Helm chart. Defaults to: presets/dev-eos-xrd-values.yaml"
  echo "      --use-public-repos:               Use the public yum repos instead of the internal yum repos. Use when you do not have access to the CERN network."
  echo "      --platform <platform>:            Which platform to build for. Defaults to the default platform in the project.json."
  echo "      --eos-enabled <true|false>:       Whether to spawn an EOS or not. Defaults to true."
  echo "      --dcache-enabled <true|false>:    Whether to spawn a dCache or not. Defaults to false."
  echo "      --local-telemetry:                Spawns a local collector and Prometheus backends to which metrics will be sent."
  echo "      --publish-telemetry:              Publishes telemetry to a pre-configured central observability backend."
  echo "      --deploy-namespace <namespace>:   Deploy the CTA instance in a given namespace. Defaults to dev."
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
  # These versions don't affect anything functionality wise
  local cta_version="5"
  local vcs_version="dev"
  local xrootd_ssi_version
  xrootd_ssi_version=$(cd "$project_root/xrootd-ssi-protobuf-interface" && git describe --tags --exact-match)

  # Input args
  local clean_build_dir=false
  local clean_build_dirs=false
  local force_install=false
  local reset=false
  local skip_build=false
  local skip_deploy=false
  local skip_cmake=false
  local skip_unit_tests=false
  local skip_debug_packages=false
  local skip_image_reload=false
  local local_telemetry=false
  local publish_telemetry=false
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
  local eos_image_tag=""
  local container_runtime="podman"
  local platform
  platform=$(jq -r .dev.defaultPlatform "${project_root}/project.json")
  local use_internal_repos=true
  local eos_enabled=true
  local dcache_enabled=false
  local enable_address_sanitizer=false


  # Parse command line arguments
  while [[ "$#" -gt 0 ]]; do
    case "$1" in
    -h | --help) usage ;;
    -r | --reset)
      reset=true
      clean_build_dirs=true
      ;;
    --clean-build-dir) clean_build_dir=true ;;
    --clean-build-dirs) clean_build_dirs=true ;;
    --disable-oracle-support) oracle_support="FALSE" ;;
    --disable-ccache) enable_ccache=false ;;
    --skip-build) skip_build=true ;;
    --skip-deploy) skip_deploy=true ;;
    --skip-cmake) skip_cmake=true ;;
    --skip-unit-tests) skip_unit_tests=true ;;
    --skip-debug-packages) skip_debug_packages=true ;;
    --skip-image-reload) skip_image_reload=true ;;
    --skip-image-cleanup) image_cleanup=false ;;
    --force-install) force_install=true ;;
    --enable-dcache) dcache_enabled=true ;;
    --disable-eos) eos_enabled=false ;;
    --upgrade-cta) upgrade_cta=true ;;
    --upgrade-eos) upgrade_eos=true ;;
    --use-public-repos) use_internal_repos=false ;;
    --local-telemetry) local_telemetry=true ;;
    --publish-telemetry) publish_telemetry=true ;;
    --enable-address-sanitizer) enable_address_sanitizer=true ;;
    --eos-image-tag)
      if [[ $# -gt 1 ]]; then
        eos_image_tag="$2"
        shift
      else
        echo "Error: --eos-image-tag requires an argument"
        usage
      fi
      ;;
    --platform)
      if [[ $# -gt 1 ]]; then
        if [[ "$(jq --arg platform "$2" '.platforms | has($platform)' "$project_root/project.json")" != "true" ]]; then
            echo "Error: platform $2 not supported. Please check the project.json for supported platforms."
        fi
        platform="$2"
        shift
      else
        echo "Error: --platform requires an argument"
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
    -c | --container-runtime)
      if [[ $# -gt 1 ]]; then
        if [[ "$2" != "docker" ]] && [[ "$2" != "podman" ]]; then
          echo "-c | --container-runtime is \"$2\" but must be one of [docker, podman]."
          exit 1
        fi
        container_runtime="$2"
        shift
      else
        echo "Error: -c | --container-runtime requires an argument"
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
    --catalogue-config)
      if [[ $# -gt 1 ]]; then
        catalogue_config="$2"
        shift
      else
        echo "Error: --catalogue-config requires an argument"
        exit 1
      fi
      ;;
    --scheduler-config)
      if [[ $# -gt 1 ]]; then
        scheduler_config="$2"
        shift
      else
        echo "Error: --scheduler-config requires an argument"
        exit 1
      fi
      ;;
    --tapeservers-config)
      if [[ $# -gt 1 ]]; then
        tapeservers_config="$2"
        shift
      else
        echo "Error: --tapeservers-config requires an argument"
        exit 1
      fi
      ;;
    --cta-config)
      if [[ $# -gt 1 ]]; then
        cta_config="$2"
        shift
      else
        echo "Error: --cta-config requires an argument"
        usage
      fi
      ;;
    --eos-config)
      if [[ $# -gt 1 ]]; then
        eos_config="$2"
        shift
      else
        echo "Error: --eos-config requires an argument"
        usage
      fi
      ;;
    --spawn-options)
      if [[ $# -gt 1 ]]; then
        extra_spawn_options+=" $2"
        shift
      else
        echo "Error: --spawn-options requires an argument"
        exit 1
      fi
      ;;
    --image-build-options)
      if [[ $# -gt 1 ]]; then
        extra_image_build_options+=" $2"
        shift
      else
        echo "Error: ---imagebuild-options requires an argument"
        exit 1
      fi
      ;;
    --deploy-namespace)
      if [[ $# -gt 1 ]]; then
        deploy_namespace="$2"
        shift
      else
        echo "Error: ---deploy-namespace requires an argument"
        exit 1
      fi
      ;;
    *)
      echo "Unsupported argument: $1"
      usage
      ;;
    esac
    shift
  done

  # navigate to root project directory
  cd "${project_root}"

  #####################################################################################################################
  # Build binaries/RPMs
  #####################################################################################################################
  if [[ "${skip_build}" = false ]]; then
    build_image_name="cta-build-image-${platform}"
    build_container_name="cta-build${project_root//\//-}-${platform}"
    # Stop and remove existing container if reset is requested
    echo "Total CTA build containers found: $(podman ps | grep -c cta-build)"
    if [[ "${reset}" = true ]]; then
      echo "Shutting down existing build container..."
      ${container_runtime} rm -f "${build_container_name}" >/dev/null 2>&1 || true
      podman rmi "${build_image_name}" > /dev/null 2>&1 || true
    fi

    # Start container if not already running
    if ${container_runtime} ps -a --format '{{.Names}}' | grep -wq "${build_container_name}"; then
      echo "Found existing build container: ${build_container_name}"
    else
      print_header "SETTING UP BUILD CONTAINER"
      restarted=true
      echo "Rebuilding build container image"
      ${container_runtime} build --no-cache -t "${build_image_name}" -f continuousintegration/docker/"${platform}"/build.Dockerfile .
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
        ./shared/CTA/continuousintegration/build/build_srpm.sh \
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

    if [[ ${restarted} = true ]] || [[ ${force_install} = true ]]; then
      # Only install srpms if it is the first time running this or if the install is forced
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

    if [[ ${use_internal_repos} = true ]]; then
      build_rpm_flags+=" --use-internal-repos"
    fi

    if [[ ${enable_address_sanitizer} = true ]]; then
      build_rpm_flags+=" --enable-address-sanitizer"
    fi

    print_header "BUILDING RPMS"
    ${container_runtime} exec -it "${build_container_name}" \
      ./shared/CTA/continuousintegration/build/build_rpm.sh \
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
      "${build_rpm_flags}"

    echo "Build successful"
  fi


  #####################################################################################################################
  # Build image
  #####################################################################################################################
  build_iteration_file=/tmp/.build_iteration
  if [[ "$skip_image_reload" == "false" ]]; then
    print_header "BUILDING CONTAINER IMAGE"
    if [[ "$upgrade_cta" == "false" ]]; then
      # Start with the tag dev-0
      local current_build_id=0
      image_tag="dev-$current_build_id"
      touch $build_iteration_file
      echo $current_build_id >$build_iteration_file

      if [[ ${image_cleanup} = true ]]; then
        # When deploying an entirely new instance, this is a nice time to clean up old images
        echo "Cleaning up unused ctageneric images..."
        minikube image ls | grep "localhost/ctageneric:dev" | xargs -r minikube image rm >/dev/null 2>&1
      fi
    else
      # This continuoully increments the image tag from previous upgrades
      if [[ ! -f "$build_iteration_file" ]]; then
        echo "Failed to find $build_iteration_file to retrieve build iteration."
        exit 1
      fi
      local current_build_id
      current_build_id=$(cat "$build_iteration_file")
      new_build_id=$((current_build_id + 1))
      image_tag="dev-$new_build_id"
      echo $new_build_id >$build_iteration_file
    fi
    if [[ ${use_internal_repos} = true ]]; then
      extra_image_build_options+=" --use-internal-repos"
    fi
    ## Create and load the new image
    local rpm_src=build_rpm/RPM/RPMS/x86_64
    echo "Building image from ${rpm_src}"
    ./continuousintegration/build/build_image.sh --tag ${image_tag} \
      --rpm-src "${rpm_src}" \
      --rpm-version "${cta_version}-${vcs_version}" \
      --container-runtime "${container_runtime}" \
      --load-into-minikube \
      "${extra_image_build_options}"
    if [[ ${image_cleanup} = true ]]; then
      # Pruning of unused images is done after image building to ensure we maintain caching
      podman image ls | grep ctageneric | grep -v "${image_tag}" | awk '{ print "localhost/ctageneric:" $2 }' | xargs -r podman rmi || true
      podman image prune -f >/dev/null
    fi
  else
    if [[ ! -f "$build_iteration_file" ]]; then
      echo "Failed to find $build_iteration_file to retrieve build iteration. Unable to identify which image to spawn/upgrade the instance with."
      exit 1
    fi
    # If we are not building a new image, use the latest one
    image_tag=dev-$(cat "$build_iteration_file")
  fi

  #####################################################################################################################
  # Deploy CTA instance
  #####################################################################################################################
  if [[ ${skip_deploy} = false ]]; then
    if [[ "$upgrade_cta" = true ]]; then
      print_header "UPGRADING CTA INSTANCE"
      cd continuousintegration/orchestration
      upgrade_options=""
      if [[ "$skip_image_reload" == "false" ]]; then
        upgrade_options+=" --cta-image-repository localhost/ctageneric --cta-image-tag ${image_tag}"
      fi
       # shellcheck disable=SC2086
      ./upgrade_cta_instance.sh --namespace "${deploy_namespace}" "${upgrade_options}" ${extra_spawn_options}
    elif [[ "$upgrade_eos" = true ]]; then
      print_header "UPGRADING EOS INSTANCE"
      cd continuousintegration/orchestration
      ./deploy_eos_instance.sh --namespace "${deploy_namespace}" --eos-image-tag "${eos_image_tag}"
    else
      print_header "DELETING OLD CTA INSTANCES"
      # By default we discard the logs from deletion as this is not very useful during development
      # and polutes the dev machine
      ./continuousintegration/orchestration/delete_instance.sh -n "${deploy_namespace}" --discard-logs
      print_header "DEPLOYING CTA INSTANCE"
      if [[ -n "${tapeservers_config}" ]]; then
        extra_spawn_options+=" --tapeservers-config ${tapeservers_config}"
      fi

      if [[ -n "${eos_image_tag}" ]]; then
        extra_spawn_options+=" --eos-image-tag ${eos_image_tag}"
      fi

      if [[ -n "${eos_config}" ]]; then
        extra_spawn_options+=" --eos-config ${eos_config}"
      fi

      if [[ -n "${cta_config}" ]]; then
        extra_spawn_options+=" --cta-config ${cta_config}"
      fi
      if [[ "$local_telemetry" = true ]]; then
        extra_spawn_options+=" --local-telemetry"
      fi
      if [[ "$publish_telemetry" = true ]]; then
        extra_spawn_options+=" --publish-telemetry"
      fi

      if [[ -z "${scheduler_config}" ]]; then
        if [[ "${scheduler_type}" == "pgsched" ]]; then
          scheduler_config="presets/dev-scheduler-postgres-values.yaml"
        else
          scheduler_config="presets/dev-scheduler-vfs-values.yaml"
        fi
      fi

      echo "Deploying CTA instance"
      cd continuousintegration/orchestration
       # shellcheck disable=SC2086
      ./create_instance.sh --namespace "${deploy_namespace}" \
        --cta-image-repository localhost/ctageneric \
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
