#!/bin/bash

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later



# Download Enstore sample tape
dnf install -y git git-lfs
git lfs install --skip-repo
git clone https://github.com/LTrestka/ens-mhvtl.git /ens-mhvtl

device="$1"
changer="${2:-/dev/smc}"
slot="${3:-2}"

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

get_mt_status() {
  local device_path="$1"
  mt -f "$device_path" status 2>/dev/null
}

wait_for_file_number_at_least() {
  local device_path="$1"
  local expected="$2"
  local timeout_seconds="${3:-120}"
  local waited=0
  local status=""
  local file_num=""

  while true; do
    status=$(get_mt_status "$device_path" || true)
    file_num=$(echo "$status" | awk 'match($0, /File number=([0-9]+)/, m) { print m[1]; exit }')
    if [[ -n "$file_num" ]] && (( file_num >= expected )); then
      return 0
    fi
    if (( waited >= timeout_seconds )); then
      echo "Timed out waiting for tape file number >= ${expected}. Last status:" >&2
      echo "$status" >&2
      return 1
    fi
    sleep 1
    waited=$((waited + 1))
  done
}

wait_for_write_ready() {
  local device_path="$1"
  local timeout_seconds="${2:-120}"
  local waited=0

  while true; do
    if dd if=/dev/zero of="$device_path" bs=1 count=0 >/dev/null 2>&1; then
      return 0
    fi
    if (( waited >= timeout_seconds )); then
      echo "Timed out waiting for tape device ${device_path} to become write-ready" >&2
      get_mt_status "$device_path" >&2 || true
      return 1
    fi
    sleep 1
    waited=$((waited + 1))
  done
}

position_to_file() {
  local device_path="$1"
  local file_number="$2"

  mt -f "$device_path" rewind
  wait_for_device_ready "$device_path" || return 1
  wait_for_file_number_at_least "$device_path" 0 || return 1

  if (( file_number > 0 )); then
    mt -f "$device_path" fsf "$file_number"
    wait_for_device_ready "$device_path" || return 1
    wait_for_file_number_at_least "$device_path" "$file_number" || return 1
  fi
}

cycle_tape_in_drive() {
  local slot_number="$1"

  mtx -f "$changer" unload "$slot_number" 0 || return 1
  sleep 2
  mtx -f "$changer" load "$slot_number" 0 || return 1
  wait_for_device_ready "$device" || return 1
}

retry_dd_on_busy() {
  local max_attempts="${DD_BUSY_MAX_ATTEMPTS:-30}"
  local sleep_seconds="${DD_BUSY_SLEEP_SECONDS:-3}"
  local max_sleep_seconds="${DD_BUSY_MAX_SLEEP_SECONDS:-10}"
  local attempt=1
  local err_file

  while true; do
    err_file=$(mktemp)
    if dd "$@" 2>"$err_file"; then
      cat "$err_file" >&2
      rm -f "$err_file"
      return 0
    fi

    if grep -qi "resource busy" "$err_file"; then
      if (( attempt >= max_attempts )); then
        cat "$err_file" >&2
        rm -f "$err_file"
        echo "dd failed with EBUSY after ${attempt} attempts" >&2
        return 1
      fi
      echo "Tape device busy, retrying dd (${attempt}/${max_attempts})..." >&2
      rm -f "$err_file"
      wait_for_device_ready "${device}" 20 || true
      sleep "${sleep_seconds}"
      if (( sleep_seconds < max_sleep_seconds )); then
        sleep_seconds=$((sleep_seconds + 1))
      fi
      attempt=$((attempt + 1))
      continue
    fi

    cat "$err_file" >&2
    rm -f "$err_file"
    return 1
  done
}

# Load tape in a tapedrive
mtx -f ${changer} status
mtx -f ${changer} load ${slot} 0
mtx -f ${changer} status

# Get the device status where the tape is loaded and rewind it.
mt -f ${device} status
wait_for_device_ready "${device}" || exit 1
position_to_file "${device}" 0 || exit 1

# Write Enstore label and payload to tape.
wait_for_device_ready "${device}" || exit 1
wait_for_write_ready "${device}" || exit 1
retry_dd_on_busy if=/ens-mhvtl/enstore/FL1212_f1/vol1_FL1212.bin of=$device bs=80 || exit 1
mt -f $device weof || exit 1
wait_for_device_ready "${device}" || exit 1
wait_for_file_number_at_least "${device}" 1 || exit 1
cycle_tape_in_drive "${slot}" || exit 1
position_to_file "${device}" 1 || exit 1
wait_for_write_ready "${device}" || exit 1
retry_dd_on_busy if=/ens-mhvtl/enstore/FL1212_f1/fseq1_payload.bin of=$device bs=1048576 || exit 1
mt -f $device weof || exit 1
wait_for_device_ready "${device}" || exit 1
wait_for_file_number_at_least "${device}" 2 || exit 1

wait_for_device_ready "${device}" || exit 1
mt -f $device rewind
wait_for_device_ready "${device}" || exit 1
