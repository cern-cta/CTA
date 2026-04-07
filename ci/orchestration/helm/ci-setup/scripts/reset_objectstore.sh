#!/bin/bash

# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -e

die() {
  echo "$@" 1>&2
  exit 1
}

echo "Using scheduler backend: $OBJECTSTORE_BACKEND"

# Clean up scheduler
if [[ "$OBJECTSTORE_BACKEND" == "vfs" ]]; then
  echo "Wiping objectstore"
  find "${SCHEDULER_URL:?}" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
  cta-objectstore-initialize "$SCHEDULER_URL" || die "ERROR: Could not wipe the objectstore. cta-objectstore-initialize $SCHEDULER_URL FAILED"
  chmod -R 777 "$SCHEDULER_URL"
elif [[ "$OBJECTSTORE_BACKEND" == "ceph" ]]; then
  echo "Wiping objectstore"
  echo "yes" | cta-objectstore-reset "$SCHEDULER_URL" || die "ERROR: Could not reset the objectstore. cta-objectstore-reset $SCHEDULER_URL FAILED"
  cta-objectstore-initialize "$SCHEDULER_URL" || die "ERROR: Could not initialize the objectstore. cta-objectstore-initialize $SCHEDULER_URL FAILED"
  echo "Rados objectstore ${SCHEDULER_URL} content:"
  cta-objectstore-list "$SCHEDULER_URL"
else
  die "ERROR: Unsupported scheduler backend: ${OBJECTSTORE_BACKEND}"
fi

echo "Scheduler reset completed"
