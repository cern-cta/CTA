#!/bin/bash

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -euo pipefail

# Download Enstore sample tape
dnf install -y git git-lfs
git lfs install --skip-repo
git clone https://github.com/LTrestka/ens-mhvtl.git /ens-mhvtl

ens_mhvtl_root="/ens-mhvtl"
layout_dir="${ens_mhvtl_root}/enstore/FL1212_f1"
device="$1"
changer="${2:-/dev/smc}"
drive_index="${3:-0}"

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

write_tape_file() {
  local input_path="$1"
  local block_size="$2"
  local description="$3"
  local rc=1

  wait_for_device_ready "${device}" || exit 1
  for attempt in {1..5}; do
    if dd if="${input_path}" of="${device}" bs="${block_size}"; then
      sleep 2
      wait_for_device_ready "${device}" || exit 1
      return 0
    fi
    rc=$?
    echo "Failed to write ${description} to ${device} (attempt ${attempt}/5), waiting for mhvtl to settle" >&2
    sleep 2
    wait_for_device_ready "${device}" || true
  done
  return "${rc}"
}

for segment in vol1_FL1212.bin fseq1_payload.bin; do
  [[ -f "${layout_dir}/${segment}" ]] || {
    echo "Missing layout segment: ${layout_dir}/${segment}" >&2
    exit 1
  }
done

# Load tape in a tapedrive
mtx -f ${changer} status
mtx -f ${changer} load 2 "${drive_index}"
mtx -f ${changer} status

# Get the device status where the tape is loaded and rewind it.
mt -f ${device} status
wait_for_device_ready "${device}" || exit 1
mt -f ${device} rewind
wait_for_device_ready "${device}" || exit 1

# Write Enstore label and payload to tape.
write_tape_file "${layout_dir}/vol1_FL1212.bin" 80 "Enstore VOL1 label"
write_tape_file "${layout_dir}/fseq1_payload.bin" 1048576 "Enstore payload"

wait_for_device_ready "$device" || exit 1
mt -f $device rewind

# give the drive time to settle
sleep 2

wait_for_device_ready "$device" || exit 1
