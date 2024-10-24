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
yum -y install mt-st lsscsi sg3_utils cta-taped cta-tape-label cta-debuginfo ceph-common

echo "Using this configuration for library:"
/opt/run/bin/init_library.sh
cat /tmp/library-rc.sh
. /tmp/library-rc.sh

ln -s /dev/${LIBRARYDEVICE} /dev/smc

/opt/run/bin/init_objectstore.sh
. /tmp/objectstore-rc.sh

echo "Configuring database"
/opt/run/bin/init_database.sh
. /tmp/database-rc.sh

echo ${DATABASEURL} > /etc/cta/cta-catalogue.conf

TAPED_CONF_FILE="/etc/cta/cta-taped-${DRIVENAMES[${driveslot}]}.conf"

# cta-taped setup
echo "taped BufferSizeBytes 262144" > "${TAPED_CONF_FILE}"
echo "taped BufferCount 200" >> "${TAPED_CONF_FILE}"
echo "taped MountCriteria 2000000, 100" >> "${TAPED_CONF_FILE}"
echo "taped WatchdogIdleSessionTimer 2" >> "${TAPED_CONF_FILE}" # Make tape servers more responsive, thus improving CI test speed
echo "ObjectStore BackendPath $OBJECTSTOREURL" >> "${TAPED_CONF_FILE}"
echo "taped UseEncryption no" >> "${TAPED_CONF_FILE}"
echo "taped DriveName ${DRIVENAMES[${driveslot}]}" >> "${TAPED_CONF_FILE}"
echo "taped DriveLogicalLibrary ${DRIVENAMES[${driveslot}]}" >> "${TAPED_CONF_FILE}"
echo "taped DriveDevice /dev/${DRIVEDEVICES[${driveslot}]}" >> "${TAPED_CONF_FILE}"
echo "taped DriveControlPath smc${driveslot}" >> "${TAPED_CONF_FILE}"
# Decrease schedulerDB cache timeout for tests
echo "taped TapeCacheMaxAgeSecs 1" >> "${TAPED_CONF_FILE}"
echo "taped RetrieveQueueCacheMaxAgeSecs 1" >> "${TAPED_CONF_FILE}"

echo "general InstanceName CI" >> "${TAPED_CONF_FILE}"
echo "general SchedulerBackendName VFS" >> "${TAPED_CONF_FILE}"


####
# configuring taped using the official location for SSS: /etc/cta/cta-taped.sss.keytab
CTATAPEDSSS="cta-taped.sss.keytab"

# key generated with 'echo y | xrdsssadmin -k taped+ -u stage -g tape  add /tmp/taped.keytab'
#echo '0 u:stage g:tape n:taped+ N:6361736405290319874 c:1481207182 e:0 f:0 k:8e2335f24cf8c7d043b65b3b47758860cbad6691f5775ebd211b5807e1a6ec84' >> /etc/cta/${CTATAPEDSSS}
chown cta /etc/cta/${CTATAPEDSSS}

cat <<EOF > /etc/sysconfig/cta-taped
CTA_TAPED_OPTIONS="--log-format=json --log-to-file=/var/log/cta/cta-taped-${DRIVENAMES[${driveslot}]}.log"
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

tail -F "/var/log/cta/cta-taped-${DRIVENAMES[${driveslot}]}.log" &

# cta-taped is ran with runuser to avoid a bug with Docker that prevents both
# the setresgid(-1, 1474, -1) and setresuid(-1, 14029, -1) system calls from
# working correctly
runuser -c "/usr/bin/cta-taped -c ${TAPED_CONF_FILE} --foreground ${CTA_TAPED_OPTIONS}"

echo "taped died"

sleep infinity

fi
