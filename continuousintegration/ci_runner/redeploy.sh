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
source "$(dirname "${BASH_SOURCE[0]}")/../ci_helpers/log_wrapper.sh"

# Help message
usage() {
  echo "Usage: $0 [options] -s|--rpm-src <rpm source>"
  echo ""
  echo "Will (re)deploy the local minikube instance with the latest rpms."
  echo "These rpms are assumed to be located in CTA/build_rpm, as done by the build_rpm.sh script in ci_helpers."
  echo "If the --skip-image-reload flag is provided, the --rpm-src is no longer mandatory."
  echo ""
  echo "options:"
  echo "  -h, --help:                           Shows help output."
  echo "  -n, --namespace <namespace>:          Specify the Kubernetes namespace. Defaults to \"dev\" if not provided."
  echo "  -t, --tag <tag>:                      Image tag to use. Defaults to \"dev\" if not provided."
  echo "  -s, --rpm-src <rpm source>:           Path to the RPMs to be installed. Can be absolute or relative to where the script is executed from. For example \"-s build_rpm/RPM/RPMS/x86_64\""
  echo "      --skip-image-reload:              Skips the step where the image is reloaded into Minikube. This allows easy redeployment with the image that is already loaded."
  echo "      --upgrade:                        Upgrade the currently running instance instead of installing it from scratch. Old values are reused unless explicitly overriden"
  echo "      --force-upgrade:                  Same as the --upgrade flag, but forces a re-deployment of all the CTA pods."
  echo "      --catalogue-config <path>:        Path to the yaml file containing the type and credentials to configure the Catalogue. Defaults to: presets/dev-catalogue-postgres-values.yaml"
  echo "      --scheduler-config <path>:        Path to the yaml file containing the type and credentials to configure the Scheduler. Defaults to: presets/dev-scheduler-vfs-values.yaml"
  echo "      --library-config <path>:          Path to the yaml file containing the library configuration. If not provided, the create_instance.sh script will autogenerate one."
  echo "      --spawn-options <options>:        Additional options to pass during pod spawning. These are passed verbatim to the create_instance script."
  echo "      --upgrade-options <options>:      Additional options to pass for the instance upgrade. These are passed verbatim to the upgrade_instance script."
  echo "      --build-options <options>:        Additional options to pass for the image building. These are passed verbatim to the build_image script."
  exit 1
}

redeploy() {
  # Default values
  local kube_namespace="dev"
  local image_tag="dev"
  local rpm_src=""
  local skip_image_reload=false
  local catalogue_config="presets/dev-catalogue-postgres-values.yaml"
  local scheduler_config="presets/dev-scheduler-vfs-values.yaml"
  local library_config=""
  local extra_spawn_options=""
  local extra_build_options=""
  local extra_upgrade_options=""
  local upgrade=false
  local force_upgrade=false

  # Parse command line arguments
  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      -h | --help) usage ;;
      --upgrade)
        upgrade=true ;;
      --force-upgrade)
        upgrade=true
        force_upgrade=true ;;
      -n | --namespace)
        if [[ $# -gt 1 ]]; then
          kube_namespace="$2"
          shift
        else
          echo "Error: -n|--namespace requires an argument"
          exit 1
        fi
        ;;
      -s | --rpm-src)
        if [[ $# -gt 1 ]]; then
          rpm_src="$2"
          shift
        else
          echo "Error: -s|--rpm-src requires an argument"
          exit 1
        fi
        ;;
      -t | --tag)
        if [[ $# -gt 1 ]]; then
          image_tag="$2"
          shift
        else
          echo "Error: -t|--tag requires an argument"
          exit 1
        fi
        ;;
      --skip-image-reload) skip_image_reload=true ;;
      --catalogue-config)
        if [[ $# -gt 1 ]]; then
          catalogue_config="$2"
          custom_catalogue=1
          shift
        else
          echo "Error: --catalogue-config requires an argument"
          exit 1
        fi
        ;;
      --scheduler-config)
        if [[ $# -gt 1 ]]; then
          scheduler_config="$2"
          custom_scheduler=1
          shift
        else
          echo "Error: --scheduler-config requires an argument"
          exit 1
        fi
        ;;
      --library-config)
        if [[ $# -gt 1 ]]; then
          library_config="$2"
          shift
        else
          echo "Error: --catalogue-config requires an argument"
          exit 1
        fi
        ;;
      --spawn-options)
        extra_spawn_options+=" $2"
        shift ;;
      --upgrade-options)
        extra_upgrade_options+=" $2"
        shift ;;
      --build-options)
        extra_build_options="$2"
        shift ;;
      *)
        echo "Unsupported argument: $1"
        usage
        ;;
    esac
    shift
  done

  if [ "$skip_image_reload" == "false" ] && [ -z "${rpm_src}" ]; then
    echo "Failure: Missing mandatory argument -s | --rpm-src"
    usage
  fi

  # Script should be run as cirunner
  if [[ $(whoami) != 'cirunner' ]]; then
    # At some point this should be improved; this script shouldn't care as long as minikube is running
    echo "Current user is $(whoami), aborting. Script must run as cirunner"
    exit 1
  fi

  ###################################################################################################

  # navigate to root directory
  cd "$(dirname "$0")"
  cd ../../

  # Delete previous instance, if it exists
  if [ "$upgrade" == "false" ] && kubectl get namespace ${kube_namespace} &>/dev/null; then
    echo "Found existing namespace \"${kube_namespace}\""
    ./continuousintegration/orchestration/delete_instance.sh -n ${kube_namespace}
  fi

  if [ "$skip_image_reload" == "false" ]; then
    ## Create and load the new image
    echo "Building image from ${rpm_src}"
    ./continuousintegration/ci_runner/build_image.sh --tag ${image_tag} \
                                                     --rpm-src "${rpm_src}" \
                                                     ${extra_build_options}
    # This step is necessary because atm podman and minikube don't share the same docker runtime and local registry
    echo "Saving image locally"
    tmpfile=$(mktemp) && trap 'rm -f $tmpfile' EXIT
    podman save -o $tmpfile localhost/ctageneric:${image_tag}
    echo "Loading new image into minikube"
    minikube image load $tmpfile --overwrite
    podman image prune -f
  fi

  instance_options=""
  if [ -n "${library_config}" ]; then
    instance_options+=" --library-config ${library_config}"
  fi
  if [ "$upgrade" == "false" ] || [ "${custom_scheduler:-0}" = "1" ] ; then
    instance_options+=" --scheduler-config ${scheduler_config}"
  fi
  if [ "$upgrade" == "false" ] || [ "${custom_catalogue:-0}" = "1" ] ; then
    instance_options+=" --catalogue-config ${catalogue_config}"
  fi
  if [ "$upgrade" == "true" ]; then
    echo "Upgrading instance"
    cd continuousintegration/orchestration
    if [ "$force_upgrade" == "true" ]; then
      instance_options+=" --force"
    fi
    ./upgrade_instance.sh --namespace ${kube_namespace} \
                          --registry-host localhost \
                          --image-tag ${image_tag} \
                          ${instance_options} \
                          ${extra_upgrade_options}
  else
    echo "Deploying instance"
    cd continuousintegration/orchestration
    ./create_instance.sh --namespace ${kube_namespace} \
                         --registry-host localhost \
                         --image-tag ${image_tag} \
                         ${instance_options} \
                         ${extra_spawn_options}
  fi
}

redeploy "$@"
