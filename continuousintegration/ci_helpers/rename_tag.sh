#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2022-2024 CERN
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

# env variables used:
# OLDTAG
# NEWTAG

rename_tag() {
  # Usage: rename_tag <old_tag> <new_tag>
  local old_tag=$1
  local new_tag=$2

  if [[ "-${old_tag}-" == "-${new_tag}-" ]]; then
    echo "The 2 tags are identical: ${old_tag}/${new_tag} no need to rename"
    exit 0
  fi

  local registry_name="cta/cta-orchestration"
  local docker_registry="gitlab-registry.cern.ch"

  local script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  local jwt_token=$(bash ${script_dir}/get_registry_credentials.sh)

  if [[ -z "$jwt_token" ]]; then
    echo "Error: Failed to retrieve JWT token."
    return 1
  fi

  echo "List of tags in registry"
  curl -H "Authorization: Bearer ${jwt_push_pull_token}" \
       "https://${docker_registry}/v2/${registry_name}/tags/list"


  echo "Pulling the manifest of tag:${old_tag}"
  curl -H "Authorization: Bearer ${jwt_push_pull_token}" \
       -H 'accept: application/vnd.docker.distribution.manifest.v2+json' \
       "https://${docker_registry}/v2/${registry_name}/manifests/${old_tag}" > manifest.json

  echo "Pushing new tag: ${new_tag}"
  curl -XPUT \
       -H "Authorization: Bearer ${jwt_push_pull_token}" \
       -H 'content-type: application/vnd.docker.distribution.manifest.v2+json' \
       -d '@manifest.json' \
       "https://${docker_registry}/v2/${registry_name}/manifests/${new_tag}" \
       -v

  echo "List of tags in registry"
  curl -H "Authorization: Bearer ${jwt_push_pull_token}" \
       "https://${docker_registry}/v2/${registry_name}/tags/list"
}

rename_tag $OLDTAG $NEWTAG
