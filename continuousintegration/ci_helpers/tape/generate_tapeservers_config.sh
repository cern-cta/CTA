#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2024 CERN
# @license      This program is free software, distributed under the terms of the GNU General Public
#               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
#               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
#               option) any later version.
#
#               This program is distributed in the hope that it will be useful, but WITHOUT ANY
#               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
#               PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
#               In applying this licence, CERN does not waive the privileges and immunities
#               granted to it by virtue of its status as an Intergovernmental Organization or
#               submit itself to any jurisdiction.

set -e

die() {
  echo "$@" 1>&2
  exit 1
}

# Function to show usage information
usage() {
  echo "Generates a tape servers configuration (as a values.yaml) containing a list of tape servers with the drives available in said library."
  echo "For each tape server, a library is specified and the drives it is responsible for."
  echo "The library configuration is based on the provided SCSI library devices and will be written to the provided target file."
  echo ""
  echo "Usage: $0 --target-file <file> --library-type --library-devices <device1,device2> [options]"
  echo ""
  echo "Options:"
  echo "  -h, --help                    Show this help message and exit."
  echo "  -o, --target-file             Path to the output the configuration file to."
  echo "  -t, --library-type            Library type to put in the configuration. E.g. mhvtl or IBM"
  echo "  -d, --library-devices         A comma separated list of library devices to generate to include in the configuration file."
  echo "  -m, --max-drives-per-tpsrv    The maximum number of drives that a single tape server can be responsible for. Defaults to 2."
  exit 1
}

# Global variable to keep track of which tape server we are inserting
tpsrv_counter=1

generate_tpsrvs_config_for_library() {
  local library_device="$1"
  local target_file="$2"
  local library_type="$3"
  # This is executed only once to ensure consistency
  local lsscsi_g="$4"
  local max_drives="$5"

  # Find the line in lsscsi output corresponding to the current library device
  local line=$(echo "$lsscsi_g" | grep "mediumx" | grep "${library_device}")
  if [ -z "$line" ]; then
    echo "Library device $library_device does not exist. Skipping..." 1>&2
    return
  fi
  if [ $(echo "$line" | wc -l) -gt 1 ]; then
    echo "Too many lines matching library device \"$library_device\" found. Ensure you passed the correct library device. Skipping..." 1>&2
    return
  fi
  local scsi_host="$(echo "$line" | sed -e 's/^.//' | cut -d\: -f1)"
  local scsi_channel="$(echo "$line" | cut -d\: -f2)"

  # Extract library name and drive names
  local library_name=$(echo "${line}" | awk '{print $4}')
  mapfile -t driveNames < <(echo "$lsscsi_g" | \
                        grep "^.${scsi_host}:${scsi_channel}:" | \
                        grep tape | \
                        sed -e 's/^.[0-9]\+:[0-9]\+:\([0-9]\+\):\([0-9]\+\)\].*/VDSTK\1\2/')

  mapfile -t driveDevices < <(echo "$lsscsi_g" | \
                        grep "^.${scsi_host}:${scsi_channel}:" | \
                        grep tape | \
                        awk '{print $6}' | \
                        sed -e 's%/dev/%n%')

  # Split driveNames and driveDevices into chunks of size max_drives
  for ((i=0; i < ${#driveNames[@]}; i+=max_drives)); do
    driveNamesChunk=( "${driveNames[@]:i:max_drives}" )
    driveDevicesChunk=( "${driveDevices[@]:i:max_drives}" )

    # Increment server counter and generate server name
    local tpsrv_name=$(printf "tpsrv%02d" "$tpsrv_counter")
    ((tpsrv_counter++))

    # Append configuration to the target file
    cat <<EOF >> "$target_file"
${tpsrv_name}:
  libraryType: "${library_type}"
  libraryDevice: "${library_device}"
  libraryName: "${library_name}"
  drives:
$(for ((j=0; j < ${#driveNamesChunk[@]}; j++)); do
  printf "    - name: \"%s\"\n      device: \"%s\"\n" "${driveNamesChunk[j]}" "${driveDevicesChunk[j]}"
done)
EOF
  done
}

generate_tpsrvs_config() {
  local target_file=""
  local library_type=""
  local library_devices=()
  local max_drives=2

  while [[ "$#" -gt 0 ]]; do
    case $1 in
      -h|--help) usage ;;
      -o|--target-file)
        target_file="$2"
        shift ;;
      -t|--library-type)
        library_type="$2"
        shift ;;
      -d|--library-devices)
        IFS=',' read -r -a library_devices <<< "$2"
        shift ;;
      -m| --max-drives-per-tpsrv)
        max_drives="$2"
        shift ;;
      *)
        echo "Unsupported argument: $1"
        usage ;;
    esac
    shift
  done

  # Ensure required arguments are provided
  if [[ -z "$target_file" || -z "$library_type" || ${#library_devices[@]} -eq 0 ]]; then
    echo "Error: --target-file, --library-type, and --library-devices are required."
    usage
  fi

  local lsscsi_g="$(lsscsi -g)"
  # Loop over each provided library device
  for library_device in "${library_devices[@]}"; do
    generate_tpsrvs_config_for_library "$library_device" "$target_file" "$library_type" "$lsscsi_g" "$max_drives"
  done
}

generate_tpsrvs_config "$@"
