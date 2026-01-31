#!/bin/bash

# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# Function to show usage information
usage() {
  echo
  echo "Lists all drives for a given library."
  echo
  echo "Usage: $0 --library-device <device> [options]"
  echo
  echo "Options:"
  echo "  -h, --help                      Show this help message and exit."
  echo "  -d, --library-device <device>   SCSI library device to put in the configuration."
  echo "  -m, --max-drives <max>          Caps the number of drives to show ."
  echo
  exit 1
}

max_drives=1000

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    -h|--help) usage ;;
    -d|--library-device)
      library_device="$2"
      shift ;;
    -m|--max-drives)
      max_drives="$2"
      shift ;;
    *)
      echo "Unsupported argument: $1"
      usage ;;
  esac
  shift
done

# Ensure required arguments are provided
if [[ -z "$library_device" ]]; then
  echo "Error: --library-device is required."
  usage
fi

lsscsi_g="$(lsscsi -g)"

# Locate the medium changer line
line=$(grep "mediumx" <<<"$lsscsi_g" | grep "$library_device") || {
  echo "Library device $library_device not found" >&2
  exit 1
}

if [[ "$line" == *$'\n'* ]]; then
  die "Multiple matches for $library_device" >&2
  exit 1
fi

# Extract SCSI host and channel
scsi_host=$(sed -E 's/^\[([0-9]+):.*/\1/' <<<"$line")
scsi_channel=$(sed -E 's/^\[[0-9]+:([0-9]+):.*/\1/' <<<"$line")


drive_devices=$(echo "$lsscsi_g" | grep "^.${scsi_host}:${scsi_channel}:" | grep tape | awk '{print $6 }')
echo '['

index=0
first=1
firstDriveNameAsLibraryForAll=""
for drive_device in $drive_devices; do
  (( index >= max_drives )) && break

  # maps /dev/stX to /dev/nstX
  nst_device="/dev/nst${drive_device#/dev/st}"
  sg_device=$(awk -v dev="$drive_device" '$0 ~ dev { print $7; exit }' <<<"$lsscsi_g")
  vendor=$(sg_inq "$sg_device" 2>/dev/null | awk '/Vendor identification/ {print $3; exit}')
  serial=$(sg_inq "$sg_device" 2>/dev/null | awk '/Unit serial number/ {print $4; exit}')

  drive_name="${vendor}-${serial}"
  if [[ -z "$firstDriveNameAsLibraryForAll" ]]; then
    firstDriveNameAsLibraryForAll=$drive_name
  fi

  (( first )) || echo ","
  first=0

  echo '  {'
  echo "    \"name\": \"${drive_name}\","
  echo "    \"device\": \"${nst_device}\","
#  echo "    \"logicalLibraryName\": \"${drive_name}\","
  echo "    \"logicalLibraryName\": \"${firstDriveNameAsLibraryForAll}MYLIB\","
  echo "    \"controlPath\": \"smc${index}\""
  echo -n '  }'

  ((index++))
done
echo
echo ']'
