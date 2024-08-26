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

  local secret_name="ctaregsecret"
  local registry_name="cta/cta-orchestration"
  local gitlab_server="gitlab.cern.ch"

  local auth_json=$(kubectl get secret $secret_name -o jsonpath='{.data.\.dockerconfigjson}' | base64 --decode | jq -r '.auths')

  local docker_registry=$(echo $auth_json | jq -r 'keys[0]')
  local docker_login_username=$(echo $auth_json | jq -r '.[].auth' | base64 --decode | cut -d: -f1)
  local docker_login_password=$(echo $auth_json | jq -r '.[].auth' | base64 --decode | cut -d: -f2)

  if [[ -z "$docker_registry" ]]; then
    echo "ERROR: Missing required variable: docker_registry"
    return 1
  fi
  if [[ -z "$docker_login_username" ]]; then
    echo "ERROR: Missing required variable: docker_login_username"
    return 1
  fi
  if [[ -z "$docker_login_password" ]]; then
    echo "ERROR: Missing required variable: docker_login_password"
    return 1
  fi

  local jwt_push_pull_token=$(curl -s -u ${docker_login_username}:${docker_login_password} \
    "https://${gitlab_server}/jwt/auth?service=container_registry&scope=repository:${registry_name}:pull,push" | jq -r '.token')

  if [[ -z "$jwt_push_pull_token" ]]; then
    echo "Error: Failed to retrieve JWT pull token."
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