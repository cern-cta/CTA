#!/bin/bash

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later



# Download Enstore sample tape
dnf install -y git git-lfs
git lfs install --skip-repo
git clone https://github.com/LTrestka/ens-mhvtl.git /ens-mhvtl

device="$1"
changer="${2:-/dev/smc}"

wait_for_device_ready() {
  local device_path="$1"
  local timeout_seconds="${2:-60}"
  local waited=0

  while true; do
    if mt -f "$device_path" status >/dev/null 2>&1; then
      return 0
    fi
    if (( waited >= timeout_seconds )); then
      echo "Timed out waiting for tape device ${device_path} to become ready" >&2
      mt -f "$device_path" status || true
      return 1
    fi
    sleep 1
    waited=$((waited + 1))
  done
}

# Load tape in a tapedrive
mtx -f ${changer} status
mtx -f ${changer} load 2 0
mtx -f ${changer} status

# Get the device status where the tape is loaded and rewind it.
mt -f ${device} status
wait_for_device_ready "${device}" || exit 1
mt -f ${device} rewind
wait_for_device_ready "${device}" || exit 1

# Write Enstore label and payload to tape.
wait_for_device_ready "${device}" || exit 1
dd if=/ens-mhvtl/enstore/FL1212_f1/vol1_FL1212.bin of=$device bs=80
mt -f $device weof
wait_for_device_ready "${device}" || exit 1
dd if=/ens-mhvtl/enstore/FL1212_f1/fseq1_payload.bin of=$device bs=1048576
mt -f $device weof

wait_for_device_ready "${device}" || exit 1
mt -f $device rewind
wait_for_device_ready "${device}" || exit 1
