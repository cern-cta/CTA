#!/bin/bash -e

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

usage() {
  echo "Usage: $0 [options] -t|--tag <image_tag> -o|--operating-system <os> -s|--rpm-src <rpm source>"
  echo ""
  echo "Builds an image based on the CTA rpms"
  echo "  -t, --tag <image_tag>:          Docker image tag. For example \"-t dev\""
  echo "  -o, --operating-system <os>:    Specifies for which operating system to build the rpms. Supported operating systems: [alma9]."
  echo "  -s, --rpm-src <rpm source>:     Path to the RPMs to be installed. Can be absolute or relative to where the script is executed from. For example \"-s build_rpm/RPM/RPMS/x86_64\""
  echo ""
  echo "Optional arguments:"
  echo "  --dockerfile <path>:            Path to the Dockerfile (default: 'alma9/Dockerfile'). Should be relative to the repository root."
  echo "  --yum-repos-dir <path>:         Directory containing yum.repos.d/ on the host. Should be relative to the repository root."
  echo "  --yum-versionlock-file <path>:  Path to versionlock.list on the host. Should be relative to the repository root."
  echo "  -h, --help:                     Shows help output."
  exit 1
}

buildImage() {

  # Default values
  local rpm_src=""
  local image_tag=""
  local operating_system=""
  local rpm_default_src="image_rpms"
  local yum_repos_dir="continuousintegration/docker/ctafrontend/alma9/etc/yum.repos.d/"
  local yum_versionlock_file="continuousintegration/docker/ctafrontend/alma9/etc/yum/pluginconf.d/versionlock.list"
  local dockerfile="continuousintegration/docker/ctafrontend/alma9/Dockerfile"

  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      -h | --help) usage ;;
      -o | --operating-system)
        if [[ $# -gt 1 ]]; then
          if [ "$2" != "alma9" ]; then
            echo "-o | --operating-system must be one of [alma9]."
            exit 1
          fi
          operating_system="$2"
          shift
        else
          echo "Error: -o | --operating-system requires an argument"
          usage
        fi
        ;;
      -s | --rpm-src)
        if [[ $# -gt 1 ]]; then
          rpm_src=$(realpath "$2")
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
      --dockerfile)
        if [[ $# -gt 1 ]]; then
          dockerfile="$2"
          shift
        else
          echo "Error: --dockerfile requires an argument"
          exit 1
        fi
        ;;
      --yum-repos-dir)
        if [[ $# -gt 1 ]]; then
          yum_repos_dir="$2"
          shift
        else
          echo "Error: --yum-repos-dir requires an argument"
          exit 1
        fi
        ;;
      --yum-versionlock-file)
        if [[ $# -gt 1 ]]; then
          yum_versionlock_file="$2"
          shift
        else
          echo "Error: --yum-versionlock-file requires an argument"
          exit 1
        fi
        ;;
      *)
        echo "Unsupported argument: $1" 
        usage ;;
    esac
    shift
  done

  if [ -z "${image_tag}" ]; then
    echo "Failure: Missing mandatory argument -t | --tag"
    usage
  fi

  if [ -z "${operating_system}" ]; then
    echo "Failure: Missing mandatory argument -o | --operating-system"
    usage
  fi

  if [ -z "${rpm_src}" ]; then
    echo "Failure: Missing mandatory argument -s | --rpm-src"
    usage
  fi

  # navigate to root directory
  cd "$(dirname "$0")"
  cd ../../

  # Copy the rpms into a predefined rpm directory
  trap 'rm -rf ${rpm_default_src}' EXIT
  mkdir -p ${rpm_default_src}
  cp -r ${rpm_src} ${rpm_default_src}

  case "${operating_system}" in
    alma9)
      echo "Running on AlmaLinux 9"
      (set -x; podman build . -f ${dockerfile} \
                              -t ctageneric:${image_tag} \
                              --network host \
                              --build-arg YUM_REPOS_DIR=${yum_repos_dir} \
                              --build-arg YUM_VERSIONLOCK_FILE=${yum_versionlock_file})
      ;;
    *)
      echo "Invalid operating system provided: ${operating_system}"
      exit 1
      ;;
  esac
}

buildImage "$@"