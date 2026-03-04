#!/bin/bash

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -euo pipefail

# Download EnstoreLarge sample tape
dnf install -y git git-lfs
git lfs install --skip-repo
git clone https://github.com/LTrestka/ens-mhvtl.git /ens-mhvtl

ens_mhvtl_root="/ens-mhvtl"
layout_dir="${ens_mhvtl_root}/enstorelarge/FL1587"
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

for segment in L1 L2 file1 file2; do
  [[ -f "${layout_dir}/${segment}" ]] || {
    echo "Missing layout segment: ${layout_dir}/${segment}" >&2
    exit 1
  }
done

# Load tape in a tapedrive
mtx -f ${changer} status
mtx -f ${changer} load 3 0
mtx -f ${changer} status

# Get the device status where the tape is loaded and rewind it.
mt -f ${device} status
wait_for_device_ready "${device}" || exit 1
mt -f ${device} rewind
wait_for_device_ready "${device}" || exit 1

# Write EnstoreLarge logical segments in deterministic order:
# label file (L1) then data file (L2 + payload segments).
wait_for_device_ready "${device}" || exit 1
dd if="${layout_dir}/L1" of="${device}" bs=80
mt -f $device weof
wait_for_device_ready "${device}" || exit 1
dd if="${layout_dir}/L2" of="${device}" bs=80
dd if="${layout_dir}/file1" of="${device}" bs=262144
dd if="${layout_dir}/file2" of="${device}" bs=262144
mt -f $device weof

wait_for_device_ready "${device}" || exit 1
mt -f $device rewind
wait_for_device_ready "${device}" || exit 1
