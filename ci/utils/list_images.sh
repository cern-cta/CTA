#!/bin/bash

# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

list_images() {
  # The Kubernetes secret stores a base64 encoded .dockerconfigjson. This json has the following format:
  # {
  #   "auths": {
  #     "gitlab-registry.cern.ch": {
  #       "auth": "base64 encoded string of 'username:password'"
  #     }
  #   }
  # }

  local registry_name="cta/ctageneric"
  local docker_registry="gitlab-registry.cern.ch"


  local script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  local jwt_token=$(bash ${script_dir}/get_registry_credentials.sh)

  if [[ -z "$jwt_token" ]]; then
    echo "Error: Failed to retrieve JWT token."
    return 1
  fi

  # List the tags in the Docker registry repository
  local list_response=$(curl -s "https://${docker_registry}/v2/${registry_name}/tags/list" -H "Authorization: Bearer ${jwt_token}")
  local tags=$(echo "$list_response" | jq -c ".tags[]" | sed -e 's/^"//;s/"$//')

  if [[ -z "$tags" ]]; then
    echo "Error: Failed to retrieve tags from repository:"
    echo "$list_response"
    return 1
  fi

  echo "$tags"
}

list_images
