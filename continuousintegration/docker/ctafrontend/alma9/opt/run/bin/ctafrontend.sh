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

. /opt/run/bin/init_pod.sh

yum-config-manager --enable cta-artifacts
yum-config-manager --enable ceph

# Install missing RPMs
# cta-catalogueutils is needed to delete the db at the end of instance
yum -y install cta-frontend cta-debuginfo cta-catalogueutils ceph-common

# /etc/cta/cta-frontend-xrootd.conf is now provided by ctafrontend rpm. It comes with CI-ready content,
# except the objectstore backend path, which we add here:

/opt/run/bin/init_objectstore.sh
. /tmp/objectstore-rc.sh

ESCAPEDURL=$(echo ${OBJECTSTOREURL} | sed 's/\//\\\//g')
sed -i "s/^.*cta.objectstore.backendpath.*$/cta.objectstore.backendpath ${ESCAPEDURL}/" /etc/cta/cta-frontend-xrootd.conf

# Set the ObjectStore URL in the ObjectStore Tools configuration

echo "ObjectStore BackendPath $OBJECTSTOREURL" >/etc/cta/cta-objectstore-tools.conf

/opt/run/bin/init_database.sh
. /tmp/database-rc.sh

echo ${DATABASEURL} >/etc/cta/cta-catalogue.conf

# EOS INSTANCE NAME used as username for SSS key
EOSINSTANCE=ctaeos

# Wait for the keytab files to be pushed in by the creation script
echo -n "Waiting for /etc/cta/eos.sss.keytab."
for ((;;)); do test -e /etc/cta/eos.sss.keytab && break; sleep 1; echo -n .; done
echo OK
echo -n "Waiting for /etc/cta/cta-frontend.krb5.keytab."
for ((;;)); do test -e /etc/cta/cta-frontend.krb5.keytab && break; sleep 1; echo -n .; done
echo OK

echo "Core files are available as $(cat /proc/sys/kernel/core_pattern) so that those are available as artifacts"

# Configuring  grpc for cta-admin tapefile disk filename resolution
echo -n "Configuring grpc to ctaeos: "
if [ -r /etc/config/eoscta/eos.grpc.keytab ]; then
  cp /etc/config/eoscta/eos.grpc.keytab /etc/cta/eos.grpc.keytab
  echo 'cta.ns.config /etc/cta/eos.grpc.keytab' >> /etc/cta/cta-frontend-xrootd.conf
  echo 'OK'
else
  echo 'KO'
fi

find /usr/lib64 | grep libXrdSsiCta.so

if [ "-${CI_CONTEXT}-" == '-nosystemd-' ]; then
  # systemd is not available
  echo 'echo "Setting environment variables for cta-frontend"' > /tmp/cta-frontend_env
  cat /etc/sysconfig/cta-frontend | sed -e 's/^/export /' >> /tmp/cta-frontend_env
  source /tmp/cta-frontend_env

  runuser --shell='/bin/bash' --session-command='cd ~cta; xrootd -l /var/log/cta-frontend-xrootd.log -k fifo -n cta -c /etc/cta/cta-frontend-xrootd.conf -I v4' cta
  echo "ctafrontend died"
  echo "analysing core file if any"
  /opt/run/bin/ctafrontend_bt.sh
  sleep infinity
else
  # Add a DNS cache on the client as kubernetes DNS complains about `Nameserver limits were exceeded`
  yum install -y systemd-resolved
  systemctl start systemd-resolved

  # systemd is available
  echo "Launching frontend with systemd:"
  systemctl start cta-frontend

  echo "Status is now:"
  systemctl status cta-frontend
fi
