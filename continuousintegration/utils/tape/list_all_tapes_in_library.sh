#!/bin/bash

# SPDX-FileCopyrightText: 2025 CERN
#
# SPDX-License-Identifier: GPL-3.0-or-later


set -e

# Function to show usage information
usage() {
  echo
  echo "Lists all tapes for a given library."
  echo
  echo "Usage: $0 --library-device <device> [options]"
  echo
  echo "Options:"
  echo "  -h, --help              Show this help message and exit."
  echo "  -d, --library-device    SCSI library device to put in the configuration."
  echo
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
