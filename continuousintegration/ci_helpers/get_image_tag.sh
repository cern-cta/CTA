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

die() {
  echo "$@" 1>&2 
  exit 1
}

usage() {
  echo "Script to automatically construct a docker image tag based on the arguments provided."
  echo ""
  echo "Usage: $0 [options]"
  echo ""
  echo "Options:"
  echo "  -h, --help:                             Show help output."
  echo "  -p|--pipeline-id <gitlab pipeline ID>:  GitLab pipeline ID (optional)."
  echo "  -S|--systemtest-only:                   Use system test image only (from the latest commit on main)."
  exit 1
}

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
  case $1 in
    -h | --help) usage ;;
    -S|--systemtest-only) systest_only=1 ;;
    -p|--pipeline-id) 
      pipeline_id="$2" 
      shift ;;
    *)
      echo "Unsupported argument: $1"
      usage
      ;;
  esac
  shift
done

# Check if registry credentials are valid
./get_registry_credentials.sh --check > /dev/null || die "Error: Credential check failed."

# Get commit ID
if [[ ${systest_only} -eq 1 ]]; then
  # In this case, grab the latest commit that is online as this will already have an image associated with it
  commit_id=$(curl --url "https://gitlab.cern.ch/api/v4/projects/139306/repository/commits" | jq -cr '.[0] | .short_id' | sed -e 's/\(........\).*/\1/')
else
  commit_id=$(git log -n1 | grep ^commit | cut -d\  -f2 | sed -e 's/\(........\).*/\1/')
fi

# Determine the image tag
if [[ "${systest_only}" -eq 1 ]]; then
  imagetag=$(./list_images.sh 2>/dev/null | grep ${commit_id} | tail -n1)
elif [ -n "${pipeline_id}" ]; then
  imagetag=$(./list_images.sh 2>/dev/null | grep ${commit_id} | grep ^${pipeline_id}git | sort -n | tail -n1)
else
  imagetag=$(./list_images.sh 2>/dev/null | grep ${commit_id} | sort -n | tail -n1)
fi

# Validate if image tag was found
if [ -z "${imagetag}" ]; then
  die "ERROR: Commit ${commit_id} has no Docker image available in the GitLab registry. Please check the pipeline status and available images."
fi

echo "${imagetag}"
