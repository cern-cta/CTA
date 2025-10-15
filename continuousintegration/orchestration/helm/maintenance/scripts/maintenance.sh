#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2022-2025 CERN
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

# install RPMs
dnf install -y cta-maintenance

# to get maintenance logs to stdout
tail -F /var/log/cta/cta-maintenance.log &
echo "$(date '+%Y-%m-%d %H:%M:%S') [$(basename "${BASH_SOURCE[0]}")] Ready"
runuser --shell='/bin/bash' --session-command="/usr/bin/cta-maintenance --foreground --log-to-file=/var/log/cta/cta-maintenance.log --log-format json --config /etc/cta/cta-maintenance.conf" cta
