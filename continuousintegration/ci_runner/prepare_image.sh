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

usage() {
  echo "Usage: $0 [options] -t|--tag <image_tag>"
  echo ""
  echo "Builds an image based on the CTA rpms"
  echo "  -t, --tag <image_tag>:        Docker image tag. For example \"-t dev\""
  echo ""
  echo "options:"
  echo "  -h, --help:                               Shows help output."
  echo "  -s, --rpm-src <rpm source>:   Path to the RPMs to be installed. Can be absolute or relative to the repository root. For example \"-s build_rpm/RPM/RPMS/x86_64\""
  exit 1
}

prepareImage() {

  # navigate to root directory
  cd "$(dirname "$0")"
  cd ../../

  # Default values
  local rpm_src=""
  local image_tag=""
  local rpm_default_src="build_rpm/RPM/RPMS/x86_64"

  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      -h | --help) usage ;;
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
      *) usage ;;
    esac
    shift
  done

  if [ -z ${image_tag} ]; then
    echo "Please specify the docker image tag"
    usage
  fi

  if [ -z ${rpm_src} ]; then
    echo "No explicit rpm source specified, using default: CTA/${rpm_default_src}"
    if [ ! -d "${rpm_default_src}" ]; then
      echo "Default rpm source not found. Please build the rpms or provide an alternative valid path."
      exit 1
    fi
  elif [ ! $(readlink -f "${rpm_src}") = $(readlink -f "${rpm_default_src}") ]; then
    trap "rm -rf ${rpm_default_src}" EXIT
    mkdir -p ${rpm_default_src}
    cp -r ${rpm_src} ${rpm_default_src}
  else
    echo "Provided rpm source is same as default."
  fi

  if [ "$(grep -c 'AlmaLinux release 9' /etc/redhat-release)" -eq 1 ]; then
    echo "Running on AlmaLinux 9"
    echo "podman build . -f continuousintegration/docker/ctafrontend/alma9/Dockerfile -t ctageneric:${image_tag} --network host"
    podman build . -f continuousintegration/docker/ctafrontend/alma9/Dockerfile -t ctageneric:${image_tag} --network host
  elif [ "$(grep -c 'CentOS Linux release 7' /etc/redhat-release)" -eq 1 ]; then
    echo "Running on CC7"
    echo "sudo docker build . -f continuousintegration/docker/ctafrontend/cc7/Dockerfile -t ctageneric:${image_tag}"
    sudo docker build . -f continuousintegration/docker/ctafrontend/cc7/Dockerfile -t ctageneric:${image_tag}
  else
    echo "Running on unsupported operating system. Only cc7 and alma9 are supported."
    exit 1
  fi
}

prepareImage "$@"
