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

ln -s /dev/${LIBRARYDEVICE} /dev/smc

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

  tail -F "/var/log/cta/cta-taped-${DRIVENAME}.log" &

  # cta-taped is ran with runuser to avoid a bug with Docker that prevents both
  # the setresgid(-1, 1474, -1) and setresuid(-1, 14029, -1) system calls from
  # working correctly
  CTA_TAPED_CONF_FILE="/etc/cta/cta-taped-${DRIVENAME}.conf"
  runuser -c "/usr/bin/cta-taped -c ${CTA_TAPED_CONF_FILE} --foreground ${CTA_TAPED_OPTIONS}"

  echo "taped died"

  sleep infinity

fi
