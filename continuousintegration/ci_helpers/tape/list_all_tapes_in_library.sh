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
  echo "Lists all tapes for a given library."
  echo ""
  echo "Usage: $0 --library-device <device> [options]"
  echo ""
  echo "Options:"
  echo "  -h, --help              Show this help message and exit."
  echo "  -d, --library-device    SCSI library device to put in the configuration."
  exit 1
}

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    -h|--help) usage ;;
    -d|--library-device)
      library_device="$2"
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

# Get the tapes currently in the library
mtx -f "/dev/${library_device}" status | \
    grep "Storage Element" | \
    grep "Full" | \
    grep -o 'VolumeTag *= *[^ ]*' | \
    grep -o '[^= ]*$' | \
    cut -c 1-6
