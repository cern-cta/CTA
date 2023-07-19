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

version=1.0.0

# If $1 is null then error message
if [ -z "$1" ]; then
    echo "Error: No argument supplied"
    echo "Usage: ./prepare_eos_builder.sh <eos_version>"
    exit 1
fi

version=$1

# Pass to docker the version of eos to build
docker build -f Dockerfile -t eosbuilder:dev --build-arg EOS_VERSION=$version .
