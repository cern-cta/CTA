#!/bin/bash
# This file must be sourced from another shell script
# . /opt/run/bin/init_pod.sh

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2022-2024 CERN
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


echo "$(date '+%Y-%m-%d %H:%M:%S') [$(basename "${BASH_SOURCE[0]}")] Started"

# At some point this logging stuff should be removed
# For delete_instance, it is not necesasry and can be done in a different way
# For monitoring, it should be relying on stdout. This is a pain with xrootd though, so we leave this here for now
LOGMOUNT=/mnt/logs

PV_PATH=""

if [ "-${MY_CONTAINER}-" != "--" ]; then
  PV_PATH="${LOGMOUNT}/${MY_NAME}/${MY_CONTAINER}"
else
  PV_PATH="${LOGMOUNT}/${MY_NAME}"
fi
mkdir -p ${PV_PATH}

echo "Copying initial /var/log content to ${PV_PATH}"
cd /var/log
tar -c . | tar -C ${PV_PATH} -xv

echo "Mounting logs volume ${PV_PATH} in /var/log"
mount --bind ${PV_PATH} /var/log

# all core dumps will go there as all the pods AND kubelet are sharing the same kernel.core_pattern
# At some point this can (and should) be done via a Daemonset, reducing the need for a container running this to be priviledged
mkdir -p /var/log/tmp
chmod 1777 /var/log/tmp
echo '/var/log/tmp/%h-%t-%e-%p-%s.core' > /proc/sys/kernel/core_pattern

# We should be able to do this using plain Kubernetes features instead
echo -n "Fixing reverse DNS for $(hostname) for xrootd: "
sed -i -c "s/^\($(hostname -i)\)\s\+.*$/\1 $(hostname -s).$(grep search /etc/resolv.conf | cut -d\  -f2) $(hostname -s)/" /etc/hosts
echo "DONE"

echo "$(date '+%Y-%m-%d %H:%M:%S') [$(basename "${BASH_SOURCE[0]}")] Finished"
