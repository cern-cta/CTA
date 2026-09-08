#!/bin/bash

# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -e

die() {
  echo "$@" 1>&2
  exit 1
}

echo "Using scheduler backend: $SCHEDULER_BACKEND"

# Clean up scheduler
if [[ "$SCHEDULER_BACKEND" == "vfs" ]] || [[ "$SCHEDULER_BACKEND" == "vfsDeprecated" ]]; then
  echo "Wiping objectstore"
  if [[ "$SCHEDULER_BACKEND" == "vfsDeprecated" ]]; then
    rm -fr $SCHEDULER_URL
    mkdir -p $SCHEDULER_URL
  else
    rm -fr "${SCHEDULER_URL:?}/*"
  fi
  cta-objectstore-initialize $SCHEDULER_URL || die "ERROR: Could not wipe the objectstore. cta-objectstore-initialize $SCHEDULER_URL FAILED"
  chmod -R 777 $SCHEDULER_URL
elif [[ "$SCHEDULER_BACKEND" == "postgres" ]]; then
  echo "Dropping the scheduler DB schema"
  echo "yes" | cta-scheduler-schema-drop /etc/cta/cta-scheduler.conf || die "ERROR: Could not drop scheduler schema. cta-scheduler-schema-drop /etc/cta/cta-scheduler.conf FAILED"
  echo "Creating the scheduler DB schema"
  cta-scheduler-schema-create /etc/cta/cta-scheduler.conf || die "ERROR: Could not create scheduler schema. cta-scheduler-schema-create /etc/cta/cta-scheduler.conf FAILED"
elif [[ "$SCHEDULER_BACKEND" == "ceph" ]]; then
  echo "Wiping objectstore"
  cta-objectstore-reset "$SCHEDULER_URL" || die "ERROR: Could not reset the objectstore. cta-objectstore-reset $SCHEDULER_URL FAILED"
  echo "Rados objectstore ${SCHEDULER_URL} content:"
  cta-objectstore-list "$SCHEDULER_URL"
else
  die "ERROR: Unsupported scheduler backend: ${SCHEDULER_BACKEND}"
fi

echo "Scheduler reset completed"
