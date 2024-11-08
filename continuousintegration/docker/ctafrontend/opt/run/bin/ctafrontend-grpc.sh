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

. /opt/run/bin/init_pod.sh

echo "$(date '+%Y-%m-%d %H:%M:%S') [$(basename "${BASH_SOURCE[0]}")] Started"

yum-config-manager --enable ceph

# Install missing RPMs
yum -y install cta-frontend-grpc cta-debuginfo ceph-common

# TODO: this should be updated once grpc is fixed
cat <<EOF > /etc/sysconfig/cta-frontend-grpc
#
# Config properties of cta-frontend-grpc
#
# port number to accept TCP connections

# change to '--tls' to enable
GRPC_USE_TLS=""
EOF

touch /CTAFRONTEND_READY
echo "$(date '+%Y-%m-%d %H:%M:%S') [$(basename "${BASH_SOURCE[0]}")] Ready"

runuser --shell='/bin/bash' --session-command='/usr/bin/cta-frontend-grpc >> /var/log/cta/cta-frontend-grpc.log' cta
echo "ctafrontend died"
sleep infinity # Keep the container alive for debugging purposes
