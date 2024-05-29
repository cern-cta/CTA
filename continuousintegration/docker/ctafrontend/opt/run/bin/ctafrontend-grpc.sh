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

yum-config-manager --enable cta-artifacts
yum-config-manager --enable ceph

# Install missing RPMs
# cta-catalogueutils is needed to delete the db at the end of instance
yum -y install cta-debuginfo cta-catalogueutils ceph-common cta-frontend-grpc


/opt/run/bin/init_objectstore.sh
. /tmp/objectstore-rc.sh


echo "ObjectStore BackendPath $OBJECTSTOREURL" >/etc/cta/cta-objectstore-tools.conf


cat <<EOF > /etc/cta/cta.conf
ObjectStore BackendPath ${OBJECTSTOREURL}
EOF

cat <<EOF > /etc/sysconfig/cta-frontend-grpc
#
# Config properties of cta-frontend-grpc
#
# port number to accept TCP connections

# change to '--tls' to enable
GRPC_USE_TLS=""
EOF


/opt/run/bin/init_database.sh
. /tmp/database-rc.sh

echo ${DATABASEURL} >/etc/cta/cta-catalogue.conf


if [ "-${CI_CONTEXT}-" == '-nosystemd-' ]; then
  # systemd is not available
  echo 'echo "Setting environment variables for cta-frontend"' > /tmp/cta-frontend_env
  cat /etc/sysconfig/cta-frontend | grep -v '^\s*\t*#' | sed -e 's/^/export /' >> /tmp/cta-frontend_env
  source /tmp/cta-frontend_env

  runuser --shell='/bin/bash' --session-command='/usr/bin/cta-frontend-grpc >> /var/log/cta/cta-frontend-grpc.log' cta
  echo "ctafrontend died"
  sleep infinity
else
   # Add a DNS cache on the client as kubernetes DNS complains about `Nameserver limits were exceeded`
  yum install -y systemd-resolved
  systemctl start systemd-resolved

  # systemd is available
  echo "Launching frontend with systemd:"
  systemctl start cta-frontend-grpc

  echo "Status is now:"
  systemctl status cta-frontend-grpc
fi
