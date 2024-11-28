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

# Note that this sets the symbolic link for ALL containers, as /dev/ is shared
# Eventually this will move to a Daemonset to ensure this happens at the node-level
# Consequently, that means that having multiple library devices is currently not supported
ln -s /dev/${LIBRARY_DEVICE} /dev/smc

# install RPMs
yum -y install mt-st mtx lsscsi sg3_utils cta-rmcd cta-smc

# to get rmcd logs to stdout
tail -F /var/log/cta/cta-rmcd.log &
echo "$(date '+%Y-%m-%d %H:%M:%S') [$(basename "${BASH_SOURCE[0]}")] Ready"
touch /RMCD_READY
runuser --user cta -- /usr/bin/cta-rmcd -f /dev/smc
