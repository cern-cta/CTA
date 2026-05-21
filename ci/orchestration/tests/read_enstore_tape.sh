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

file_sha256() {
  sha256sum "$1" | awk '{ print $1 }'
}

files_match() {
  [[ "$(file_sha256 "$1")" == "$(file_sha256 "$2")" ]]
}

dump_file_prefix() {
  od -An -tx1 -N 64 "$1" >&2 || true
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
    else
      rc=$?
    fi
    echo "Failed to write ${description} to ${device} (attempt ${attempt}/5), waiting for mhvtl to settle" >&2
    sleep 2
    wait_for_device_ready "${device}" || true
  done
  return "${rc}"
}

read_tape_file() {
  local output_path="$1"
  local block_size="$2"
  local count="$3"
  local description="$4"
  local rc=1

  wait_for_device_ready "${device}" || exit 1
  for attempt in {1..5}; do
    if dd if="${device}" of="${output_path}" bs="${block_size}" count="${count}"; then
      return 0
    else
      rc=$?
    fi
    echo "Failed to read ${description} from ${device} (attempt ${attempt}/5), waiting for mhvtl to settle" >&2
    sleep 2
    wait_for_device_ready "${device}" || true
  done
  return "${rc}"
}

space_filemarks_forward() {
  local count="$1"
  local description="$2"
  local rc=1

  wait_for_device_ready "${device}" || exit 1
  for attempt in {1..5}; do
    if mt -f "${device}" fsf "${count}"; then
      sleep 2
      wait_for_device_ready "${device}" || exit 1
      return 0
    else
      rc=$?
    fi
    echo "Failed to position to ${description} on ${device} (attempt ${attempt}/5), waiting for mhvtl to settle" >&2
    sleep 2
    wait_for_device_ready "${device}" || true
  done
  return "${rc}"
}

verify_enstore_tape_layout() {
  local tmpdir
  local payload_size
  local payload_blocks
  tmpdir=$(mktemp -d)
  payload_size=$(stat -c%s "${layout_dir}/fseq1_payload.bin")
  payload_blocks=$(( (payload_size + 262144 - 1) / 262144 ))

  wait_for_device_ready "${device}" || exit 1
  mt -f "${device}" rewind
  wait_for_device_ready "${device}" || exit 1

  read_tape_file "${tmpdir}/vol1.bin" 80 1 "Enstore VOL1 label"
  if ! files_match "${layout_dir}/vol1_FL1212.bin" "${tmpdir}/vol1.bin"; then
    echo "Enstore VOL1 label read back from ${device} does not match input" >&2
    dump_file_prefix "${tmpdir}/vol1.bin"
    rm -rf "${tmpdir}"
    exit 1
  fi

  mt -f "${device}" rewind
  wait_for_device_ready "${device}" || exit 1
  space_filemarks_forward 1 "Enstore payload"

  read_tape_file "${tmpdir}/payload.bin" 262144 "${payload_blocks}" "Enstore payload"
  if [[ "$(stat -c%s "${tmpdir}/payload.bin")" -ne "${payload_size}" ]]; then
    echo "Enstore payload readback from ${device} has the wrong size" >&2
    ls -l "${layout_dir}/fseq1_payload.bin" "${tmpdir}/payload.bin" >&2 || true
    rm -rf "${tmpdir}"
    exit 1
  fi
  if ! files_match "${layout_dir}/fseq1_payload.bin" "${tmpdir}/payload.bin"; then
    echo "Enstore payload read back from ${device} does not match input" >&2
    dump_file_prefix "${tmpdir}/payload.bin"
    rm -rf "${tmpdir}"
    exit 1
  fi

  mt -f "${device}" rewind
  wait_for_device_ready "${device}" || exit 1
  rm -rf "${tmpdir}"
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
# The Enstore reader uses a 1 MiB read buffer but accepts shorter tape blocks.
# The mhvtl CI runner consistently rejects the 1 MiB write with EBUSY here,
# while 256 KiB blocks match the EnstoreLarge CI write path and are readable.
write_tape_file "${layout_dir}/fseq1_payload.bin" 262144 "Enstore payload"
verify_enstore_tape_layout
