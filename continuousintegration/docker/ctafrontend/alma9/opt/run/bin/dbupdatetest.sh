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

# This libraries are needed to install oracle-instant-client
# (TO BE FIXED: with the current population of repositories in CI the standard CC7 repos are not available in the container: this should be fixed in the container adding repos for these 2)
yum install --assumeyes wget libaio

# Create wrapper to use the entrypoint of the container
echo "
#/bin/bash
COMMAND=\$1
/entrypoint.sh -d -f \"$CATALOGUE_SOURCE_VERSION\" -t \"$CATALOGUE_DESTINATION_VERSION\" -i \"$COMMIT_ID\" -c \"\$COMMAND\"
" &> /launch_liquibase.sh
chmod +x /launch_liquibase.sh

echo "dbupdatetest pod is ready"
#sleep 10 minutes
sleep 600
