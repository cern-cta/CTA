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

# Function to show usage information
usage() {
  echo "Generates a library configuration (as a values.yaml) with the library configuration."
  echo "The library configuration is based on the provided SCSI library device"
  echo ""
  echo "Usage: $0 --target-file <file> --library-device <device> [options]"
  echo ""
  echo "Options:"
  echo "  -h, --help              Show this help message and exit."
  echo "  -o, --target-file       Path to the output the configuration file to."
  echo "  -d, --library-device    SCSI library device to put in the configuration."
  echo "  -d, --library-type      Library type to put in the configuration."
  exit 1
}

# Generates a values.yaml file with the library configuration
generate_library_config() {
  local target_file="$1"
  local library_device="$2"
  local library_type="$3"
  local lsscsi_g="$(lsscsi -g)"
  
  local line=$(echo "$lsscsi_g" | grep "${library_device}")
  local scsi_host="$(echo "$line" | sed -e 's/^.//' | cut -d\: -f1)"
  local scsi_channel="$(echo "$line" | cut -d\: -f2)"
  
  # Get drive names
  local drivenames=$(echo "$lsscsi_g" | \
                    grep "^.${scsi_host}:${scsi_channel}:" | \
                    grep tape | \
                    sed -e 's/^.[0-9]\+:[0-9]\+:\([0-9]\+\):\([0-9]\+\)\].*/VDSTK\1\2/' | \
                    paste -sd' ' -)

  # Get drive devices
  local drivedevices=$(echo "$lsscsi_g" | \
                      grep "^.${scsi_host}:${scsi_channel}:" | \
                      grep tape | \
                      awk '{print $6}' | \
                      sed -e 's%/dev/%n%' | \
                      paste -sd' ' -)

  
  # Get the tapes currently in the library
  local tapes=$(mtx -f "/dev/${library_device}" status | \
                grep "Storage Element" | \
                grep "Full" | \
                grep -o 'VolumeTag *= *[^ ]*' | \
                grep -o '[^= ]*$' | \
                cut -c 1-6 | \
                paste -sd' ' -)

  # Generate the values.yaml configuration
  cat <<EOF > "$target_file"
library:
  type: "${library_type}"
  name: "$(echo ${line} | awk '{print $4}')"
  device: "${library_device}"
  drivenames:
$(for name in ${drivenames}; do echo "    - \"${name}\""; done)
  drivedevices:
$(for device in ${drivedevices}; do echo "    - \"${device}\""; done)
  tapes:
$(for tape in ${tapes}; do echo "    - \"${tape}\""; done)
EOF
}

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
  case $1 in
    -h|--help) usage ;;
    -o|--target-file)
      target_file="$2"
      shift ;;
    -d|--library-device)
      library_device="$2"
      shift ;;
    -t|--library-type)
      library_type="$2"
      shift ;;
    *)
      echo "Unsupported argument: $1"
      usage ;;
  esac
  shift
done

# Ensure required arguments are provided
if [[ -z "$target_file" || -z "$library_device" || -z "$library_type" ]]; then
  echo "Error: --target-file, --library-device and --library-type are required."
  usage
fi

# Call the function with parsed arguments
generate_library_config "$target_file" "$library_device" "$library_type"
