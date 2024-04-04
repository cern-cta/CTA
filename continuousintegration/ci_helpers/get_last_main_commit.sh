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
#
# set in /etc/gitlab/gitlabregistry.txt managed by Puppet
. /etc/gitlab/gitlabregistry.txt

TO=gitlab-registry.cern.ch/cta/ctageneric

CI_REGISTRY=$(echo ${TO} | sed -e 's%/.*%%')
REPOSITORY=$(echo ${TO} | sed -e 's%[^/]\+/%%')

GITLAB_HOST=gitlab.cern.ch

JWT_PULL_PUSH_TOKEN=$(curl -q -u ${DOCKER_LOGIN_USERNAME}:${DOCKER_LOGIN_PASSWORD}    "https://${GITLAB_HOST}/jwt/auth?service=container_registry&scope=repository:${REPOSITORY}:pull,push" | cut -d\" -f4 )

curl "https://${CI_REGISTRY}/v2/${REPOSITORY}/commits" -H "Authorization: Bearer ${JWT_PULL_PUSH_TOKEN}"  | jq -c -r '.[0] | .shortid'
