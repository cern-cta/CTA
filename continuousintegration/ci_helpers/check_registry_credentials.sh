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

check_credentials() {
  # The Kubernetes secret stores a base64 encoded .dockerconfigjson. This json has the following format:
  # {
  #   "auths": {
  #     "gitlab-registry.cern.ch": {
  #       "auth": "base64 encoded string of 'username:password'"
  #     }
  #   }
  # }

  local secret_name="ctaregsecret"
  local registry_name="cta/ctageneric"
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

  # Retrieve JWT pull token from GitLab
  local jwt_pull_token=$(curl -s -u "${docker_login_username}:${docker_login_password}" \
    "https://${gitlab_server}/jwt/auth?service=container_registry&scope=repository:${registry_name}:pull,push" | jq -r '.token')

  if [[ -z "$jwt_pull_token" ]]; then
    echo "Error: Failed to retrieve JWT pull token."
    echo "\tRegistry: $docker_registry"
    echo "\tUsername: $docker_login_username"
    return 1
  fi

  echo "Credentials verified"
  return 0
}

check_credentials
