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

if [ ! -e /etc/buildtreeRunner ]; then
  yum-config-manager --enable cta-artifacts
  yum-config-manager --enable ceph
  yum-config-manager --enable castor

  # Install missing RPMs
  yum -y install mt-st lsscsi sg3_utils cta-taped cta-tape-label cta-debuginfo ceph-common oracle-instantclient-tnsnames.ora
fi

echo "Using this configuration for library:"
/opt/run/bin/init_library.sh
cat /tmp/library-rc.sh
. /tmp/library-rc.sh

ln -s /dev/${LIBRARYDEVICE} /dev/smc

# tpconfig="${DRIVENAMES[${driveslot}]} ${LIBRARYNAME} /dev/${DRIVEDEVICES[${driveslot}]} smc${driveslot}"
# Configuring one library per tape drive so that mhvtl can work with multiple tapeservers
tpconfig="${DRIVENAMES[${driveslot}]} ${DRIVENAMES[${driveslot}]} /dev/${DRIVEDEVICES[${driveslot}]} smc${driveslot}"

/opt/run/bin/init_objectstore.sh
. /tmp/objectstore-rc.sh

echo "Configuring database"
/opt/run/bin/init_database.sh
. /tmp/database-rc.sh

echo ${DATABASEURL} > /etc/cta/cta-catalogue.conf

# cta-taped setup
  echo "taped BufferSizeBytes 262144" > /etc/cta/cta-taped.conf
  echo "taped BufferCount 200" >> /etc/cta/cta-taped.conf
  echo "taped MountCriteria 2000000, 100" >> /etc/cta/cta-taped.conf
  echo "ObjectStore BackendPath $OBJECTSTOREURL" >> /etc/cta/cta-taped.conf
  echo "taped UseEncryption no" >> /etc/cta/cta-taped.conf
  echo "${tpconfig}" > /etc/cta/TPCONFIG

####
# configuring taped using the official location for SSS: /etc/cta/cta-taped.sss.keytab
CTATAPEDSSS="cta-taped.sss.keytab"

# key generated with 'echo y | xrdsssadmin -k taped+ -u stage -g tape  add /tmp/taped.keytab'
#echo '0 u:stage g:tape n:taped+ N:6361736405290319874 c:1481207182 e:0 f:0 k:8e2335f24cf8c7d043b65b3b47758860cbad6691f5775ebd211b5807e1a6ec84' >> /etc/cta/${CTATAPEDSSS}
echo -n '0 u:daemon g:daemon n:ctaeos+ N:6361884315374059521 c:1481241620 e:0 f:0 k:1a08f769e9c8e0c4c5a7e673247c8561cd23a0e7d8eee75e4a543f2d2dd3fd22' > /etc/cta/${CTATAPEDSSS}
chmod 600 /etc/cta/${CTATAPEDSSS}
chown cta /etc/cta/${CTATAPEDSSS}

cat <<EOF > /etc/sysconfig/cta-taped
CTA_TAPED_OPTIONS="--log-to-file=/var/log/cta/cta-taped.log"
XrdSecPROTOCOL=sss
XrdSecSSSKT=/etc/cta/${CTATAPEDSSS}
EOF


if [ "-${CI_CONTEXT}-" == '-systemd-' ]; then
  # systemd is available
  echo "Launching rmcd with systemd:"
  systemctl start cta-taped

  echo "Status is now:"
  systemctl status cta-taped

  # Add a DNS cache on the client as kubernetes DNS complains about `Nameserver limits were exceeded`
  yum install -y systemd-resolved
  systemctl start systemd-resolved

else
  # systemd is not available

. /etc/sysconfig/cta-taped
export XrdSecPROTOCOL
export XrdSecSSSKT

tail -F /var/log/cta/cta-taped.log &

# cta-taped is ran with runuser to avoid a bug with Docker that prevents both
# the setresgid(-1, 1474, -1) and setresuid(-1, 14029, -1) system calls from
# working correctly
runuser -c "/usr/bin/cta-taped --foreground ${CTA_TAPED_OPTIONS}"

echo "taped died"

sleep infinity

fi
