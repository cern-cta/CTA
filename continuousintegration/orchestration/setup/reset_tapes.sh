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

usage() {
  echo "Unloads any tapes still in the drives. This concerns all drives belonging to the library devices in use by the provided namespace."
  echo ""
  echo "Usage: $0 [options]"
  echo ""
  echo "Options:"
  echo "  -h, --help:              Show help output."
  echo "  -n|--namespace:     The kubernetes namespaces to execute this in."
  exit 1
}

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
  case $1 in
    -h | --help) usage ;;
    -n|--namespace)
      namespace="$2"
      shift ;;
    *)
      echo "Unsupported argument: $1"
      usage
      ;;
  esac
  shift
done

if [ -z "$namespace" ]; then
  echo "Missing mandatory argument: -n | --namespace" 1>&2
  exit 1
fi

devices=$(kubectl get all --namespace $namespace -l cta/library-device -o jsonpath='{.items[*].metadata.labels.cta/library-device}' | tr ' ' '\n' | sort | uniq)

for library_device in $devices; do
  echo "Unloading tapes that could be remaining in the drives from previous runs for library device: $library_device"
  mtx -f /dev/${library_device} status
  for unload in $(mtx -f /dev/${library_device}  status | grep '^Data Transfer Element' | grep -vi ':empty' | sed -e 's/Data Transfer Element /drive/;s/:.*Storage Element /-slot/;s/ .*//'); do
    # normally, there is no need to rewind with virtual tapes...
    mtx -f /dev/${library_device} unload $(echo ${unload} | sed -e 's/^.*-slot//') $(echo ${unload} | sed -e 's/drive//;s/-.*//') || echo "COULD NOT UNLOAD TAPE"
  done
done
