#!/bin/bash -e

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2023 CERN
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

image_tag="dev"
if [ -n "$1" ]; then
    version_arg="--build-arg EOS_VERSION=$1"
    image_tag="${image_tag}-$1"
fi

# Pass to docker the version of EOS to build
podman build --platform linux/amd64 -f Dockerfile -t eosdev:${image_tag} ${version_arg} .
