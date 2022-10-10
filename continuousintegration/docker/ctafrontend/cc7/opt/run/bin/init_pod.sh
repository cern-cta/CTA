# This file must be sourced from another shell script
# . /opt/run/bin/init_pod.sh

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

LOGMOUNT=/mnt/logs

# Setting pod fqdn from kubernetes internal NS
# this is idempotent and works for both internal and externally resolved pod DNS names
SHORTHOSTNAME=$(hostname -s)
DNSHOSTNAME=$(nslookup -type=A ${SHORTHOSTNAME}  | egrep '^Name' | awk '{print $2}')
echo "SHORTHOSTNAME:${SHORTHOSTNAME} resolved in ${DNSHOSTNAME}"
echo ${DNSHOSTNAME} > /etc/hostname
hostname --file /etc/hostname

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
mkdir -p /var/log/tmp
chmod 1777 /var/log/tmp
echo '/var/log/tmp/%h-%t-%e-%p-%s.core' > /proc/sys/kernel/core_pattern

# Creating the Rados performance locking file
touch /var/log/cta-rados-locking.log
#Creating the symbolic link to /var/log/cta-rados-locking.log
ln -s /var/log/cta-rados-locking.log /var/tmp/cta-rados-locking.log

echo -n "Fixing reverse DNS for $(hostname) for xrootd: "
sed -i -c "s/^\($(hostname -i)\)\s\+.*$/\1 $(hostname -s).$(grep search /etc/resolv.conf | cut -d\  -f2) $(hostname -s)/" /etc/hosts
echo "DONE"

# Not needed anymore, keep it in case it comes back
echo -n "Yum should resolve names using IPv4 DNS: "
echo "ip_resolve=IPv4" >> /etc/yum.conf
echo "DONE"

# defining CI_CONTEXT
# possible values are "systemd" and "nosystemd"
# this is just to understand if the container is managed through systemd or not
CI_CONTEXT="nosystemd"
if [ "-$(cat /proc/1/cmdline 2>&1 | sed -e 's/\x0//g;s/init.*/init/')-" == '-/usr/sbin/init-' ]; then
  CI_CONTEXT="systemd"
fi


# redefining yum commands if in BUILDTREE environment
if [[ -n ${BUILDTREE_BASE} ]]; then
  echo "Configuring BUILDTREE environment"
  yum-config-manager() { echo "Skipping yum-config-manager $@"; }
  yum() { echo "Skipping yum $@"; }
  /opt/run/bin/mkSymlinks.sh

  # cta:tape needed for ctafrontend and taped but adding it inconditionally in buildtree
  echo "Adding cta user and group"
  /usr/bin/getent passwd cta || /usr/sbin/useradd -s /bin/nologin -c "CTA system account" -g tape cta
  ## The following is not working as one cannot refresh groups in current shell...
  # echo 'Adding cta in tape group (grant access to /dev/sg* /dev/st* /dev/nst* devices'
  # /usr/sbin/usermod -a -G tape cta
  # Chmod is the only way to go
  chmod 666 /dev/nst* /dev/st* /dev/sg*
  # creating /var/log/cta needed by taped
  mkdir -p /var/log/cta
  chmod a+rwx /var/log/cta
else
  # some yum optimisations for the standard system
  SQUID_PROXY=squid.kube-system.svc.cluster.local
  ping -W 1 -c1 ${SQUID_PROXY} &>/dev/null && yum() { echo "Using SQUID proxy ${SQUID_PROXY}"; http_proxy=${SQUID_PROXY}:3128 /usr/bin/yum $@; }
fi
