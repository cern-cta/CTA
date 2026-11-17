#!/bin/bash

# SPDX-FileCopyrightText: 2024 CERN
# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -euo pipefail

config=/etc/cta/cta-scheduler.conf
case "$SCHEDULER_BACKEND" in
  postgres)
    echo yes | cta-scheduler-schema-drop "$config"
    cta-scheduler-schema-create "$config"
    ;;
  vfs)
    url=$(cat "$config")
    # Only remove contents of the mounted store, including hidden files, on explicit reset.
    [[ "$url" == /objectstore ]] || {
      echo "Refusing to initialize unexpected objectstore path: $url" >&2
      exit 1
    }
    mkdir -p "$url"
    find "$url" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
    cta-objectstore-initialize "$url"
    chmod -R 777 "$url"
    ;;
  ceph)
    url=$(cat "$config")
    echo yes | cta-objectstore-reset "$url"
    cta-objectstore-initialize "$url"
    ;;
  *) echo "Unsupported scheduler backend: $SCHEDULER_BACKEND" >&2; exit 1 ;;
esac
