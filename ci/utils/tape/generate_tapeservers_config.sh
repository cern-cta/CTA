#!/bin/bash

# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -e

die() {
  echo "$@" 1>&2
  exit 1
}

# Function to show usage information
usage() {
  echo
  echo "Generates a tape servers configuration (as a values.yaml) containing a list of tape servers with the drives available in said library."
  echo "For each tape server, a library is specified and the drives it is responsible for."
  echo "The library configuration is based on the provided SCSI library devices and will be written to the provided target file."
  echo
  echo "Usage: $0 --target-file <file> --library-type --library-devices <device1,device2> [options]"
  echo
  echo "Options:"
  echo "  -h, --help                    Show this help message and exit."
  echo "  -o, --target-file             Path to the output the configuration file to."
  echo "  -t, --library-type            Library type to put in the configuration. E.g. mhvtl or IBM"
  echo "  -d, --library-devices         A comma separated list of library devices to generate to include in the configuration file."
  echo "  -m, --max-drives-per-tpsrv    The maximum number of drives that a single tape server can be responsible for. Defaults to 2."
  echo "      --max-tapeservers         The maximum number of tape servers that will be present in the config. Defaults to 2."
  echo
  exit 1
}

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

# Global variable to keep track of which tape server we are inserting
tpsrv_counter=1

generate_tpsrvs_config() {
  local max_drives_per_tpsrv=1
  local max_tape_servers=2

  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      -h|--help) usage ;;
      -m|--max-drives-per-tpsrv)
        max_drives_per_tpsrv="$2"
        shift ;;
      --max-tapeservers)
        max_tape_servers="$2"
        shift ;;
      *)
        echo "Unsupported argument: $1" >&2
        usage ;;
    esac
    shift
  done

  local lsscsi_g
  lsscsi_g="$(lsscsi -g)"

  while read -r obj; do
    # Hard cap on number of servers
    [[ "$tpsrv_counter" -gt "$max_tape_servers" ]] && break

    local library_id library_device
    library_id=$(jq -r '.id' <<<"$obj")
    library_device=$(jq -r '.device' <<<"$obj")

    # Locate the medium changer line
    local line
    line=$(grep "mediumx" <<<"$lsscsi_g" | grep "$library_device") || {
      echo "Library device $library_device not found, skipping" >&2
      continue
    }

    if [[ "$line" == *$'\n'* ]]; then
      echo "Multiple matches for $library_device, skipping" >&2
      continue
    fi

    # Extract SCSI host and channel
    local scsi_host scsi_channel
    scsi_host=$(sed -E 's/^\[([0-9]+):.*/\1/' <<<"$line")
    scsi_channel=$(sed -E 's/^\[[0-9]+:([0-9]+):.*/\1/' <<<"$line")

    # Collect drives (device|name pairs)
    mapfile -t drives < <(
      grep "^.${scsi_host}:${scsi_channel}:" <<<"$lsscsi_g" \
      | grep tape \
      | awk '{print $6 "|" $3}'
    )

    # Chunk drives into tape servers
    for ((i=0; i < ${#drives[@]}; i+=max_drives_per_tpsrv)); do
      [[ "$tpsrv_counter" -gt "$max_tape_servers" ]] && break

      local tpsrv_name
      tpsrv_name=$(printf "tpsrv%02d" "$tpsrv_counter")

      echo "${tpsrv_name}:"
      echo "  libraryId: \"${library_id}\""
      echo "  libraryDevice: \"${library_device}\""
      echo "  drives:"

      for ((j=i; j < i + max_drives_per_tpsrv && j < ${#drives[@]}; j++)); do
        local drive_device drive_name
        drive_device=${drives[j]%%|*}
        drive_name=${drives[j]#*|}

        echo "    - name: \"${drive_name}\""
        echo "      device: \"${drive_device}\""
      done

      ((tpsrv_counter++))
    done

  done < <("$SCRIPT_DIR/list_all_libraries.sh" | jq -c '.[]')
}


generate_tpsrvs_config "$@"
