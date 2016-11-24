#!/bin/bash

# env variables used:
# DOCKER_LOGIN_USERNAME
# DOCKER_LOGIN_PASSWORD
# OLDTAG
# NEWTAG

# TO=gitlab-registry.cern.ch/cta/cta-orchestration

CI_REGISTRY=$(echo ${TO} | sed -e 's%/.*%%')
REPOSITORY=$(echo ${TO} | sed -e 's%[^/]\+/%%')

GITLAB_HOST=gitlab.cern.ch

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
