#!/bin/bash

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later



# Download EnstoreLarge sample tape
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
mtx -f ${changer} load 3 0
mtx -f ${changer} status

# Get the device status where the tape is loaded and rewind it.
mt -f ${device} status
wait_for_device_ready "${device}" || exit 1
mt -f ${device} rewind
wait_for_device_ready "${device}" || exit 1

# Write EnstoreLarge label, header, payload, and trailer to tape.
wait_for_device_ready "${device}" || exit 1
dd if=/ens-mhvtl/enstorelarge/FL1587_f1/vol1_FL1587.bin of=$device bs=80
mt -f $device weof
wait_for_device_ready "${device}" || exit 1
dd if=/ens-mhvtl/enstorelarge/FL1587_f1/fseq1_header.bin of=$device bs=262144
mt -f $device weof
wait_for_device_ready "${device}" || exit 1
dd if=/ens-mhvtl/enstorelarge/FL1587_f1/fseq1_payload.bin of=$device bs=262144
mt -f $device weof
wait_for_device_ready "${device}" || exit 1
dd if=/ens-mhvtl/enstorelarge/FL1587_f1/fseq1_trailer.bin of=$device bs=262144
mt -f $device weof

wait_for_device_ready "${device}" || exit 1
mt -f $device rewind
wait_for_device_ready "${device}" || exit 1
