#!/bin/bash

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -euo pipefail

dnf install -y git git-lfs psmisc

git lfs install --skip-repo

ens_mhvtl_root="${ENS_MHVTL_ROOT:-/ens-mhvtl}"
if [[ -d "${ens_mhvtl_root}/.git" ]]; then
  git -C "${ens_mhvtl_root}" lfs pull
else
  git clone https://github.com/LTrestka/ens-mhvtl.git "${ens_mhvtl_root}"
  git -C "${ens_mhvtl_root}" lfs pull
fi

# Use preloaded Enstore sample tape files.
vol1="${ens_mhvtl_root}/enstore/FL1212_f1/vol1_FL1212.bin"
payload="${ens_mhvtl_root}/enstore/FL1212_f1/fseq1_payload.bin"

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

list_payload_contents() {
  local payload_path="$1"

  echo "Listing ROOT payload contents from ${payload_path}"
  if [[ ! -f "${payload_path}" ]]; then
    echo "Payload file not found: ${payload_path}"
    return 1
  fi

  if head -n 1 "${payload_path}" | grep -q "version https://git-lfs.github.com/spec/v1"; then
    echo "Payload is a Git LFS pointer. Run: git -C ${ens_mhvtl_root} lfs pull"
    return 1
  fi

  ls -lh "${payload_path}"
  file "${payload_path}" || true

  show_sample_payload() {
    local sample_path="$1"
    echo "Sample payload detected; ROOT directory keys are missing."
    echo "Showing raw bytes (first 512 bytes) and printable strings."
    if command -v xxd >/dev/null 2>&1; then
      xxd -g1 -c16 -l 512 "${sample_path}" || true
    else
      hexdump -C -n 512 "${sample_path}" || true
    fi
    if command -v strings >/dev/null 2>&1; then
      strings -n 8 "${sample_path}" | head -n 50 || true
    fi
  }

  if command -v python3 >/dev/null 2>&1; then
    PAYLOAD_PATH="${payload_path}" python3 - <<'PY'
import os
import struct
import sys

path = os.environ.get("PAYLOAD_PATH")
with open(path, "rb") as f:
    header = f.read(64)

if header[:4] != b"root":
    print("Payload is not a ROOT file.")
    sys.exit(5)

size = os.path.getsize(path)
fend = struct.unpack(">i", header[12:16])[0]
if fend > size:
    print(f"Payload appears truncated: ROOT header fEND={fend} bytes, file size={size} bytes.")
    print("This looks like a sample payload; ROOT directory keys are at the end and are missing.")
    sys.exit(3)

try:
    import uproot
except Exception as exc:
    print(f"uproot is not available: {exc}")
    sys.exit(2)

try:
    f = uproot.open(path)
except Exception as exc:
    print(f"Failed to open ROOT payload: {exc}")
    sys.exit(4)

print("ROOT payload keys:")
for key in f.keys():
    print(key)
sys.exit(0)
PY
    status=$?
    if [[ ${status} -eq 0 ]]; then
      return 0
    fi
    if [[ ${status} -eq 3 ]]; then
      show_sample_payload "${payload_path}"
      return 0
    fi
    if [[ ${status} -eq 5 ]]; then
      return 1
    fi
    if [[ ${status} -eq 2 ]]; then
      if ! command -v pip3 >/dev/null 2>&1; then
        dnf install -y python3-pip
      fi
      pip3 install --no-cache-dir uproot
      PAYLOAD_PATH="${payload_path}" python3 - <<'PY'
import os
import uproot

path = os.environ.get("PAYLOAD_PATH")
f = uproot.open(path)
print("ROOT payload keys:")
for key in f.keys():
    print(key)
PY
      return $?
    fi
  fi

  if command -v rootls >/dev/null 2>&1; then
    rootls -l "${payload_path}" || rootls "${payload_path}"
    return 0
  fi

  echo "Unable to list payload contents: ROOT or python3+uproot not available."
  return 1
}

# Load tape in a tapedrive
mtx -f /dev/smc status
mtx -f /dev/smc load 2 ${drive_index}
mtx -f /dev/smc status

# Get the device status where the tape is loaded and rewind it.
ensure_device_free "${device}" 60
mt -f ${device} status || true
mt -f ${device} rewind || true
mt -f ${device} status || true

# Write Enstore label and payload to tape.
label_block_size=80
label_blocks=2

touch /enstore-tape.img
write_with_retry "Writing VOL1 label block" dd if="${vol1}" of=/enstore-tape.img bs=${label_block_size} count=1  # Build the first VOL1 label block.
truncate -s $((label_block_size * label_blocks)) /enstore-tape.img  # Pad the label file to two blocks (OSM-style multi-block label).
write_with_retry "Writing VOL1 label to tape" dd if=/enstore-tape.img of="${device}" bs=${label_block_size} count=${label_blocks}  # Write both label blocks to the tape drive.
mt -f ${device} status || true
ensure_device_free "${device}" 60

list_payload_contents "${payload}" || true
write_with_retry "Writing payload to tape" dd if="${payload}" of="${device}" bs=1048576  # Stream the Enstore payload to tape in 1 MiB blocks.

mt -f $device rewind || true
