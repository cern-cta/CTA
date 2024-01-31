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

# the only argument of this script is the CI_CONTEXT passed by ctaeos-mgm.sh
CI_CONTEXT=$1

# I need 2 level of directories for quarkdb:
# /var/lib/<possible PV mount point>/<directory owned by xrootd>
# this way I can either use:
# - a PV that mounts a dedicated SSD for long tests
# - a directory in the container for shorter ones

QUARKDB_CONFIG=/etc/config/eos/xrootd-quarkdb.cfg

QUARKDB_DIRECTORY=$(cat ${QUARKDB_CONFIG} | grep redis.database | sed -e 's/.*redis.database\ \+//;s%/\ *$%%')

QUARKDB_SUBDIRECTORY=$(echo ${QUARKDB_DIRECTORY} | sed -e 's%/[^/]\+/*\ *$%%')

# make sure the first level of directory exists if there is no mounted PV
mkdir -p ${QUARKDB_SUBDIRECTORY}

yum-config-manager --enable eos-quarkdb

yum install -y quarkdb

quarkdb-create --path ${QUARKDB_DIRECTORY}

chown -R xrootd:xrootd ${QUARKDB_DIRECTORY}

cp -f ${QUARKDB_CONFIG} /etc/xrootd/xrootd-quarkdb.cfg

# quarkdb is starting as xrootd user and mgm as daemon
# the password file must be 400 for each service...
# for now copy and chown, later run quarkdb as daemon and use /etc/eos.keytab for both
cp /etc/eos.keytab /etc/eos.keytab.xrootd
chmod 400 /etc/eos.keytab.xrootd
chown xrootd:xrootd /etc/eos.keytab.xrootd

if [ "-${CI_CONTEXT}-" == '-systemd-' ]; then

# using xrootd server start script with eos-xrootd override...
# we really need quarkdb start scripts...
mkdir -p /etc/systemd/system/xrootd\@quarkdb.service.d
cat <<EOF > /etc/systemd/system/xrootd\@quarkdb.service.d/custom.conf
[Service]
ExecStart=
ExecStart=/opt/eos/xrootd/bin/xrootd -l /var/log/xrootd/xrootd.log -c /etc/xrootd/xrootd-%i.cfg -k fifo -s /var/run/xrootd/xrootd-%i.pid -n %i
EOF
  # apply this custom override
  systemctl daemon-reload

  systemctl start xrootd@quarkdb

  systemctl status xrootd@quarkdb
else
  # no systemd
  XRDPROG=/usr/bin/xrootd; test -e /opt/eos/xrootd/bin/xrootd && XRDPROG=/opt/eos/xrootd/bin/xrootd
  ${XRDPROG} -n quarkdb -l /var/log/xrootd/xrootd.log -c /etc/xrootd/xrootd-quarkdb.cfg -k fifo -s /var/run/xrootd/xrootd-quarkdb.pid -b -Rxrootd
fi
