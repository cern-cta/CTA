#!/bin/bash

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

JWT_PULL_PUSH_TOKEN=$(curl -q -u ${DOCKER_LOGIN_USERNAME}:${DOCKER_LOGIN_PASSWORD} \
  "https://${GITLAB_HOST}/jwt/auth?service=container_registry&scope=repository:${REPOSITORY}:pull,push" | cut -d\" -f4 )

# echo "List of tags in registry"
curl "https://${CI_REGISTRY}/v2/${REPOSITORY}/tags/list" -H "Authorization: Bearer ${JWT_PULL_PUSH_TOKEN}" | jq -c ".tags[]" | sed -e 's/^"//;s/"$//'
