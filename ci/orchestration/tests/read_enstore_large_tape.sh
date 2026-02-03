#!/bin/bash

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -euo pipefail

# Download EnstoreLarge sample tape

dnf install -y git git-lfs psmisc

git lfs install --skip-repo

ens_mhvtl_root="${ENS_MHVTL_ROOT:-/ens-mhvtl}"
if [[ -d "${ens_mhvtl_root}/.git" ]]; then
  git -C "${ens_mhvtl_root}" lfs pull
else
  git clone https://github.com/LTrestka/ens-mhvtl.git "${ens_mhvtl_root}"
  git -C "${ens_mhvtl_root}" lfs pull
fi

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <tape-device>" >&2
  exit 1
fi

device="$1"
resolved_device=$(readlink -f "${device}" 2>/dev/null || echo "${device}")
drive_index=0
if [[ "${resolved_device}" =~ ([0-9]+)$ ]]; then
  drive_index="${BASH_REMATCH[1]}"
fi

vol1="${ens_mhvtl_root}/enstorelarge/FL1587_f1/vol1_FL1587.bin"
header="${ens_mhvtl_root}/enstorelarge/FL1587_f1/fseq1_header.bin"
payload="${ens_mhvtl_root}/enstorelarge/FL1587_f1/fseq1_payload.bin"
trailer="${ens_mhvtl_root}/enstorelarge/FL1587_f1/fseq1_trailer.bin"

ensure_device_free() {
  local dev="$1"
  local max_attempts="${2:-60}"
  local attempt=1

  while (( attempt <= max_attempts )); do
    if dd if="${dev}" of=/dev/null bs=1 count=0 >/dev/null 2>&1; then
      return 0
    fi
    echo "Tape device ${dev} busy (attempt ${attempt}/${max_attempts})"
    report_device_users "${dev}"
    sleep 2
    attempt=$((attempt + 1))
  done

  echo "Timed out waiting for ${dev} to be free" >&2
  return 1
}

report_device_users() {
  local dev="$1"
  if command -v fuser >/dev/null 2>&1; then
    fuser -v "${dev}" || true
    if [[ "${FUSER_KILL_BUSY:-1}" == "1" ]]; then
      fuser -k "${dev}" || true
    fi
  fi
  if command -v lsof >/dev/null 2>&1; then
    lsof "${dev}" || true
  fi
}

write_with_retry() {
  local desc="$1"
  shift
  local max_attempts="${WRITE_RETRY_MAX:-10}"
  local sleep_seconds="${WRITE_RETRY_SLEEP:-2}"
  local attempt=1
  local errfile
  errfile=$(mktemp)

  while true; do
    if "$@" 2>"${errfile}"; then
      rm -f "${errfile}"
      return 0
    fi

    local err
    err=$(cat "${errfile}")
    if echo "${err}" | grep -q "Device or resource busy"; then
      echo "${desc} failed: device busy (attempt ${attempt}/${max_attempts})"
      report_device_users "${device}"
      sleep "${sleep_seconds}"
      ensure_device_free "${device}" 30 || true
      attempt=$((attempt + 1))
      if (( attempt > max_attempts )); then
        echo "${err}" >&2
        rm -f "${errfile}"
        return 1
      fi
      continue
    fi

    echo "${err}" >&2
    rm -f "${errfile}"
    return 1
  done
}

# Load tape in a tapedrive
mtx -f /dev/smc status
mtx -f /dev/smc load 1 ${drive_index}
mtx -f /dev/smc status

# Get the device status where the tape is loaded and rewind it.
ensure_device_free "${device}" 60
mt -f ${device} status || true
mt -f ${device} rewind || true
mt -f ${device} status || true

# Write EnstoreLarge label, header, payload, and trailer to tape.
label_block_size=80
label_blocks=2

touch /enstorelarge-tape.img
write_with_retry "Writing VOL1 label block" dd if="${vol1}" of=/enstorelarge-tape.img bs=${label_block_size} count=1  # Build the first VOL1 label block.
truncate -s $((label_block_size * label_blocks)) /enstorelarge-tape.img  # Pad the label file to two blocks (OSM-style multi-block label).
write_with_retry "Writing VOL1 label to tape" dd if=/enstorelarge-tape.img of="${device}" bs=${label_block_size} count=${label_blocks}  # Write both label blocks to the tape drive.
mt -f ${device} status || true
ensure_device_free "${device}" 60
write_with_retry "Writing header to tape" dd if="${header}" of="${device}" bs=262144  # Write the EnstoreLarge file header block to tape (256 KiB blocks).
mt -f ${device} status || true
ensure_device_free "${device}" 60
write_with_retry "Writing payload to tape" dd if="${payload}" of="${device}" bs=262144  # Write the EnstoreLarge payload to tape (256 KiB blocks).
mt -f ${device} status || true
ensure_device_free "${device}" 60
write_with_retry "Writing trailer to tape" dd if="${trailer}" of="${device}" bs=262144  # Write the EnstoreLarge trailer block to tape (256 KiB blocks).

mt -f ${device} rewind || true
