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
  echo "Calls create_instance.sh with some reasonable defaults. A CTA image tag must be provided."
  echo "Usage: $0 --cta-image-tag <tag> [options]"
  echo
  echo "options:"
  echo "  -h, --help:                           Shows help output."
  echo "      --cta-image-tag:                  Image to use for spawning CTA."
  echo "      --eos-image-tag:                  Image to use for spawning EOS. If not provided, will default to the image specified in the create_instance script."
  echo "      --spawn-options <options>:        Additional options to pass for the deployment. These are passed verbatim to the create/upgrade instance scripts."
  echo "      --cta-config <path>:              Custom Values file to pass to the CTA Helm chart. Defaults to: presets/dev-cta-xrd-values.yaml"
  echo "      --eos-config <path>:              Custom Values file to pass to the EOS Helm chart. Defaults to: presets/dev-eos-values.yaml"
  echo "      --scheduler-config <path>:        Path to the yaml file containing the type and credentials to configure the Scheduler. Defaults to: presets/dev-scheduler-vfs-values.yaml"
  echo "      --catalogue-config <path>:        Path to the yaml file containing the type and credentials to configure the Catalogue. Defaults to: presets/dev-catalogue-postgres-values.yaml"
  echo "      --tapeservers-config <path>:      Path to the yaml file containing the tapeservers config. If not provided, this will be auto-generated."
  echo
  exit 1
}

deploy() {

  # Input args
  local eos_image_tag=""
  local cta_image_tag=""
  local cta_config="presets/dev-cta-xrd-values.yaml"
  local eos_config="presets/dev-eos-values.yaml"
  local catalogue_config="presets/dev-catalogue-postgres-values.yaml"
  local scheduler_config="presets/dev-scheduler-vfs-values.yaml"
  local extra_spawn_options=""

  # Defaults
  local deploy_namespace="dev"

  # Parse command line arguments
  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      -h | --help) usage ;;
      --cta-image-tag)
        if [[ $# -gt 1 ]]; then
          cta_image_tag="$2"
          shift
        else
          echo "Error: --cta-image-tag requires an argument"
          usage
        fi
        ;;
      --eos-image-tag)
        if [[ $# -gt 1 ]]; then
          eos_image_tag="$2"
          shift
        else
          echo "Error: --eos-image-tag requires an argument"
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
      *)
        echo "Unsupported argument: $1"
        usage
        ;;
    esac
    shift
  done

  if [ -z "${cta_image_tag}" ]; then
    echo "Missing mandatory argument: --cta-image-tag"
    usage
  fi

  # Navigate to root directory
  local project_root=$(git rev-parse --show-toplevel)
  cd "$project_root"

  print_header "DELETING OLD CTA INSTANCES"
  # By default we discard the logs from deletion as this is not very useful during development
  # and polutes the dev machine
  ./continuousintegration/orchestration/delete_instance.sh -n ${deploy_namespace} --discard-logs
  print_header "DEPLOYING CTA INSTANCE"
  if [ -n "${tapeservers_config}" ]; then
    extra_spawn_options+=" --tapeservers-config ${tapeservers_config}"
  fi

  if [ -n "${eos_image_tag}" ]; then
    extra_spawn_options+=" --eos-image-tag ${eos_image_tag}"
  fi

  if [ -n "${eos_config}" ]; then
    extra_spawn_options+=" --eos-config ${eos_config}"
  fi

  if [ -n "${cta_config}" ]; then
    extra_spawn_options+=" --cta-config ${cta_config}"
  fi

  echo "Deploying CTA instance"
  cd continuousintegration/orchestration
  ./create_instance.sh --namespace ${deploy_namespace} \
                      --cta-image-tag ${cta_image_tag} \
                      --catalogue-config ${catalogue_config} \
                      --scheduler-config ${scheduler_config} \
                      --reset-catalogue \
                      --reset-scheduler \
                      ${extra_spawn_options}

}

deploy "$@"
