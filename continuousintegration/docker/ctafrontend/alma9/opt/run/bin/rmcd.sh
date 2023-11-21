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
yum-config-manager --enable castor

# source library configuration file
echo "Using this configuration for library:"
/opt/run/bin/init_library.sh
cat /tmp/library-rc.sh
. /tmp/library-rc.sh

ln -s /dev/${LIBRARYDEVICE} /dev/smc


if [ "-${CI_CONTEXT}-" == '-systemd-' ]; then
  # systemd is available

cat <<EOF >/etc/sysconfig/cta-rmcd
DAEMON_COREFILE_LIMIT=unlimited
CTA_RMCD_OPTIONS=/dev/smc
EOF

  # install RPMs
  yum -y install mt-st mtx lsscsi sg3_utils cta-rmcd cta-smc

  # rmcd will be running as non root user, we need to fix a few things:
  # device access rights
  chmod 666 /dev/${LIBRARYDEVICE}

  echo "Launching cta-rmcd with systemd:"
  systemctl start cta-rmcd

  echo "Status is now:"
  systemctl status cta-rmcd

else
  # systemd is not available
  # install RPMs?
  yum -y install mt-st mtx lsscsi sg3_utils cta-rmcd cta-smc

  # to get rmcd logs to stdout
  tail -F /var/log/cta/cta-rmcd.log &

  runuser --user cta -- /usr/bin/cta-rmcd -f /dev/smc

fi
