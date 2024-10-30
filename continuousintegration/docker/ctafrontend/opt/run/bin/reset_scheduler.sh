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
echo "$(date '+%Y-%m-%d %H:%M:%S') [$(basename "${BASH_SOURCE[0]}")] Started"

die() {
  stdbuf -i 0 -o 0 -e 0 echo "$@"
  sleep 1
  exit 1
}

# enable cta repository from previously built artifacts
yum-config-manager --enable cta-artifacts

# install the needed packages
# the scheduler tools are installed once the scheduler type is known (see below)

if [ "$SCHEDULER_BACKEND" == "postgres" ]; then
    echo "Installing the cta-scheduler-utils"
    yum -y install cta-scheduler-utils
else
    yum -y install ceph-common
    yum-config-manager --enable ceph
    echo "Installing the cta-objectstore-tools"
    yum -y install cta-objectstore-tools
fi

echo "Using scheduler backend: $SCHEDULER_BACKEND"

# Clean up scheduler
if [ "$SCHEDULER_BACKEND" == "VFS" ]; then
  echo "Wiping objectstore"
  rm -fr $SCHEDULER_URL
  mkdir -p $SCHEDULER_URL
  cta-objectstore-initialize $SCHEDULER_URL || die "ERROR: Could not wipe the objectstore. cta-objectstore-initialize $SCHEDULER_URL FAILED"
  chmod -R 777 $SCHEDULER_URL
elif [ "$SCHEDULER_BACKEND" == "postgres" ]; then
  echo "Postgres scheduler config file content: "
  cat /etc/cta/cta-scheduler.conf
  echo "Droping the scheduler DB schema"
  echo "yes" | cta-scheduler-schema-drop /etc/cta/cta-scheduler.conf || die "ERROR: Could not drop scheduler schema. cta-scheduler-schema-drop /etc/cta/cta-scheduler.conf FAILED"
  echo "Creating the scheduler DB schema"
  cta-scheduler-schema-create /etc/cta/cta-scheduler.conf || die "ERROR: Could not create scheduler schema. cta-scheduler-schema-create /etc/cta/cta-scheduler.conf FAILED"
else
  echo "Wiping objectstore"
  if [[ $(rados -p $SCHEDULER_CEPH_POOL --id $SCHEDULER_CEPH_ID --namespace $SCHEDULER_CEPH_NAMESPACE ls | wc -l) -gt 0 ]]; then
    echo "Rados objectstore ${SCHEDULER_URL} is not empty: deleting content"
    rados -p $SCHEDULER_CEPH_POOL --id $SCHEDULER_CEPH_ID --namespace $SCHEDULER_CEPH_NAMESPACE ls | xargs -L 100 -P 100 rados -p $SCHEDULER_CEPH_POOL --id $SCHEDULER_CEPH_ID --namespace $SCHEDULER_CEPH_NAMESPACE rm
  fi
  cta-objectstore-initialize $SCHEDULER_URL || die "ERROR: Could not wipe the objectstore. cta-objectstore-initialize $SCHEDULER_URL FAILED"
  echo "Rados objectstore ${SCHEDULER_URL} content:"
  rados -p $SCHEDULER_CEPH_POOL --id $SCHEDULER_CEPH_ID --namespace $SCHEDULER_CEPH_NAMESPACE ls
fi

echo "### SCHEDULER RESET COMPLETED ###"
echo "$(date '+%Y-%m-%d %H:%M:%S') [$(basename "$0")] Done"
