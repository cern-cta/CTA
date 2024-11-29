#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2022-2024 CERN
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

. /opt/run/bin/init_pod.sh
echo "$(date '+%Y-%m-%d %H:%M:%S') [$(basename "${BASH_SOURCE[0]}")] Started"

# Install missing RPMs
dnf install -y cta-cli cta-immutable-file-test xrootd-client eos-client

echo "$(date '+%Y-%m-%d %H:%M:%S') [$(basename "${BASH_SOURCE[0]}")] Ready"
# sleep forever but exit immediately when pod is deleted
exec /bin/bash -c "trap : TERM INT; sleep infinity & wait"
