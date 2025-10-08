#!/bin/bash

# SPDX-FileCopyrightText: 2025 CERN
# SPDX-License-Identifier: GPL-3.0-or-later


die() {
  echo "$@" 1>&2
  exit 1
}

usage() {
  echo
  echo "Script to automatically find the latest docker image tag on a given branch."
  echo
  echo "Usage: $0 [options]"
  echo
  echo "Options:"
  echo "  -h, --help:             Show help output."
  echo "  -b|--branch branch:     The branch on which to find the latest image tag."
  echo
  exit 1
}

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    -h | --help) usage ;;
    -b|--branch)
      branch="$2"
      shift ;;
    *)
      echo "Unsupported argument: $1"
      usage
      ;;
  esac
  shift
done

if [ -z "$branch" ]; then
  die "Please provide a branch to find the latest image tag for"
fi

# Navigate to script location
cd "$(dirname "${BASH_SOURCE[0]}")"

# Check if registry credentials are valid
./get_registry_credentials.sh --check > /dev/null || die "Error: Credential check failed."

# Get commit ID
# Grab the latest commit that is online on the given branch as this will already have an image associated with it
commit_id=$(curl --silent --url "https://gitlab.cern.ch/api/v4/projects/139306/repository/commits?ref_name=${branch}" \
          | jq -cr '.[0].short_id' \
          | sed -e 's/\(........\).*/\1/')

imagetag=$(./list_images.sh 2>/dev/null | grep ${commit_id} | tail -n1)

# Validate if image tag was found
if [ -z "${imagetag}" ]; then
  die "ERROR: Commit ${commit_id} has no Docker image available in the GitLab registry. Please check the pipeline status and available images."
fi

echo "${imagetag}"
