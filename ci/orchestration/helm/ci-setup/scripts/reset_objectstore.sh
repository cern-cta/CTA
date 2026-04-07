#!/bin/bash

# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -e

# TODO: remove this file once the objectstore is removed

echo "Using objectstore backend: $OBJECTSTORE_BACKEND"

# Clean up scheduler
if [[ "$OBJECTSTORE_BACKEND" == "vfs" ]]; then
  echo "Installing the cta-objectstore-tools"
  dnf install -y cta-objectstore-tools
  echo "Wiping objectstore"
  rm -fr "${SCHEDULER_URL:?}/*"
  cta-objectstore-initialize $SCHEDULER_URL
  chmod -R 777 $SCHEDULER_URL
elif [[ "$OBJECTSTORE_BACKEND" == "ceph" ]]; then
  echo "Installing the cta-objectstore-tools"
  dnf config-manager --enable ceph
  dnf install -y cta-objectstore-tools ceph-common
  echo "Wiping objectstore"
  if [[ $(rados -p $SCHEDULER_CEPH_POOL --id $SCHEDULER_CEPH_ID --namespace $SCHEDULER_CEPH_NAMESPACE ls | wc -l) -gt 0 ]]; then
    echo "Rados objectstore ${SCHEDULER_URL} is not empty: deleting content"
    rados -p $SCHEDULER_CEPH_POOL --id $SCHEDULER_CEPH_ID --namespace $SCHEDULER_CEPH_NAMESPACE ls | xargs -L 100 -P 100 rados -p $SCHEDULER_CEPH_POOL --id $SCHEDULER_CEPH_ID --namespace $SCHEDULER_CEPH_NAMESPACE rm
  fi
  cta-objectstore-initialize $SCHEDULER_URL
  echo "Rados objectstore ${SCHEDULER_URL} content:"
  rados -p $SCHEDULER_CEPH_POOL --id $SCHEDULER_CEPH_ID --namespace $SCHEDULER_CEPH_NAMESPACE ls
else
  die "ERROR: Unsupported objectstore backend: ${OBJECTSTORE_BACKEND}"
fi

echo "Scheduler reset completed"
