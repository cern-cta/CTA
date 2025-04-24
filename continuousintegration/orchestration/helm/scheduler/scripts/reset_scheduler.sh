#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2024 CERN
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

set -e

echo "$(date '+%Y-%m-%d %H:%M:%S') [$(basename "${BASH_SOURCE[0]}")] Started"

die() {
  echo "$@" 1>&2
  exit 1
}

echo "Using scheduler backend: $SCHEDULER_BACKEND"

# Clean up scheduler
if [ "$SCHEDULER_BACKEND" == "vfs" ] || [ "$SCHEDULER_BACKEND" == "vfsDeprecated" ]; then
  echo "Installing the cta-objectstore-tools"
  dnf install -y cta-objectstore-tools valgrind
  echo "Wiping objectstore"
  if [ "$SCHEDULER_BACKEND" == "vfsDeprecated" ]; then
    rm -fr $SCHEDULER_URL
    mkdir -p $SCHEDULER_URL
  else
    rm -fr "${SCHEDULER_URL:?}/*"
  fi
  cta-objectstore-initialize $SCHEDULER_URL || die "ERROR: Could not wipe the objectstore. cta-objectstore-initialize $SCHEDULER_URL FAILED"
  chmod -R 777 $SCHEDULER_URL
elif [ "$SCHEDULER_BACKEND" == "postgres" ]; then
  echo "Installing the cta-scheduler-utils"
  dnf install -y cta-scheduler-utils
  echo "Postgres scheduler config file content: "
  cat /etc/cta/cta-scheduler.conf
  echo "Droping the scheduler DB schema"
  echo "yes" | cta-scheduler-schema-drop /etc/cta/cta-scheduler.conf || die "ERROR: Could not drop scheduler schema. cta-scheduler-schema-drop /etc/cta/cta-scheduler.conf FAILED"
  echo "Creating the scheduler DB schema"
  cta-scheduler-schema-create /etc/cta/cta-scheduler.conf || die "ERROR: Could not create scheduler schema. cta-scheduler-schema-create /etc/cta/cta-scheduler.conf FAILED"
elif [ "$SCHEDULER_BACKEND" == "ceph" ]; then
  echo "Installing the cta-objectstore-tools"
  dnf config-manager --enable ceph
  dnf install -y cta-objectstore-tools ceph-common
  echo "Wiping objectstore"
  if [[ $(rados -p $SCHEDULER_CEPH_POOL --id $SCHEDULER_CEPH_ID --namespace $SCHEDULER_CEPH_NAMESPACE ls | wc -l) -gt 0 ]]; then
    echo "Rados objectstore ${SCHEDULER_URL} is not empty: deleting content"
    rados -p $SCHEDULER_CEPH_POOL --id $SCHEDULER_CEPH_ID --namespace $SCHEDULER_CEPH_NAMESPACE ls | xargs -L 100 -P 100 rados -p $SCHEDULER_CEPH_POOL --id $SCHEDULER_CEPH_ID --namespace $SCHEDULER_CEPH_NAMESPACE rm
  fi
  cta-objectstore-initialize $SCHEDULER_URL || die "ERROR: Could not wipe the objectstore. cta-objectstore-initialize $SCHEDULER_URL FAILED"
  echo "Rados objectstore ${SCHEDULER_URL} content:"
  rados -p $SCHEDULER_CEPH_POOL --id $SCHEDULER_CEPH_ID --namespace $SCHEDULER_CEPH_NAMESPACE ls
else
  die "ERROR: Unsupported scheduler backend: ${SCHEDULER_BACKEND}"
fi

echo "$(date '+%Y-%m-%d %H:%M:%S') [$(basename "$0")] Scheduler reset completed"
