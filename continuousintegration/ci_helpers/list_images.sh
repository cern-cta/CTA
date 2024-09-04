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
