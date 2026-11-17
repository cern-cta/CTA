#!/bin/bash

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -euo pipefail

connection=$(cat "$1")
case "$connection" in
  postgresql:*) connection=${connection#postgresql:} ;;
  oracle:*)
    # Oracle catalogues are externally provisioned, not started by ci-infra.
    echo "Using externally managed Oracle catalogue."
    exit 0
    ;;
  *) echo "Unsupported database connection type in $1" >&2; exit 1 ;;
esac

# Query with the reset credentials; accepting TCP connections alone is insufficient.
# Keep credentials and transient connection diagnostics out of the log.
export PGCONNECT_TIMEOUT=2
until psql "$connection" --no-psqlrc --set=ON_ERROR_STOP=1 --command='SELECT 1' >/dev/null 2>&1; do
  echo "Waiting for database connection..."
  sleep 1
done
echo "Database connection ready."
