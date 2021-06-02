#!/bin/bash

# @project        The CERN Tape Archive (CTA)
# @copyright      Copyright(C) 2021 CERN
# @license        This program is free software: you can redistribute it and/or modify
#                 it under the terms of the GNU General Public License as published by
#                 the Free Software Foundation, either version 3 of the License, or
#                 (at your option) any later version.
#
#                 This program is distributed in the hope that it will be useful,
#                 but WITHOUT ANY WARRANTY; without even the implied warranty of
#                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#                 GNU General Public License for more details.
#
#                 You should have received a copy of the GNU General Public License
#                 along with this program.  If not, see <http://www.gnu.org/licenses/>.

. /opt/run/bin/init_pod.sh

if [ ! -e /etc/buildtreeRunner ]; then
  yum-config-manager --enable cta-artifacts
  yum-config-manager --enable ceph
  yum-config-manager --enable castor
fi

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

  # install RPMs?
  test -e /etc/buildtreeRunner || yum -y install mt-st mtx lsscsi sg3_utils cta-rmcd cta-smc

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
  test -e /etc/buildtreeRunner || yum -y install mt-st mtx lsscsi sg3_utils cta-rmcd cta-smc

  # to get rmcd logs to stdout
  tail -F /var/log/cta/cta-rmcd.log &

  runuser --user cta -- /usr/bin/cta-rmcd -f /dev/smc

fi
