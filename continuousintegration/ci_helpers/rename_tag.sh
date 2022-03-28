#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2022 CERN
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
# DOCKER_LOGIN_USERNAME
# DOCKER_LOGIN_PASSWORD
# OLDTAG
# NEWTAG

# TO=gitlab-registry.cern.ch/cta/cta-orchestration

CI_REGISTRY=$(echo ${TO} | sed -e 's%/.*%%')
REPOSITORY=$(echo ${TO} | sed -e 's%[^/]\+/%%')

GITLAB_HOST=gitlab.cern.ch

if [[ "-${OLDTAG}-" == "-${NEWTAG}-" ]]; then
  echo "The 2 tags are identical: ${OLDTAG}/${NEWTAG} no need to rename"
  exit 0
fi

JWT_PULL_PUSH_TOKEN=$(curl -q -u ${DOCKER_LOGIN_USERNAME}:${DOCKER_LOGIN_PASSWORD} \
  "https://${GITLAB_HOST}/jwt/auth?service=container_registry&scope=repository:${REPOSITORY}:pull,push" | cut -d\" -f4 )

echo "List of tags in registry"
curl "https://${CI_REGISTRY}/v2/${REPOSITORY}/tags/list" -H "Authorization: Bearer ${JWT_PULL_PUSH_TOKEN}"


echo "Pulling the manifest of tag:${OLDTAG}"
curl "https://${CI_REGISTRY}/v2/${REPOSITORY}/manifests/${OLDTAG}" -H "Authorization: Bearer ${JWT_PULL_PUSH_TOKEN}" -H 'accept: application/vnd.docker.distribution.manifest.v2+json' > manifest.json

echo "Pushing new tag: ${NEWTAG}"
curl -XPUT "https://${CI_REGISTRY}/v2/${REPOSITORY}/manifests/${NEWTAG}" -H "Authorization: Bearer ${JWT_PULL_PUSH_TOKEN}" -H 'content-type: application/vnd.docker.distribution.manifest.v2+json' -d '@manifest.json' -v

echo "List of tags in registry"
curl "https://${CI_REGISTRY}/v2/${REPOSITORY}/tags/list" -H "Authorization: Bearer ${JWT_PULL_PUSH_TOKEN}"
