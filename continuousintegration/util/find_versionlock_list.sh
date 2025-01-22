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
    echo "Script to find the path to the version lock file in the repo."
    echo ""
    echo "Usage: $0 -t <tag_value>"
    echo ""
    echo "Options:"
    echo "  -t, --tag:          Tag on which to find the version lock  file."
    echo "  -h, --help:         Show help output."
    exit 1
}

# Parse command line argumetns
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    -h | --help) usage ;;
    -t | --tag)
      tag="$2"
      shift ;;
    *)
      echo "Unsupported argument :$1"
      usage
      ;;
  esac
  shift
done

working_branch=$(git branch --show-current)
# Checkout target branch.
git checkout tags/"${tag}" --quiet

# Should match for version > 5.11.2.0-1
if [ -d "continuousintegration/docker/alma9/etc/yum/pluginconf.d/" ]; then
  echo "continuousintegration/docker/alma9/etc/yum/pluginconf.d/versionlock.list"
# Should match for version >= 5.11.1.0-1
elif [ -d "continuousintegration/docker/ctafrontend/alma9/etc/yum/pluginconf.d/" ]; then
  echo "continuousintegration/docker/ctafrontend/alma9/etc/yum/pluginconf.d/versionlock.list"
# should match for versions <= 5.11.0.1-1
elif [ -d "continuousintegration/docker/ctafrontend/alma9/yum/pluginconf.d" ]; then
  echo "continuousintegration/docker/ctafrontend/alma9/yum/pluginconf.d/versionlock.list"
else
  die "ERROR: Could not find the version lock list."
fi

# Go back to commit branch.
git checkout "${CI_COMMIT_SHORT_SHA}" --quiet
