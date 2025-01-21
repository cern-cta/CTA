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
  echo "Configures an EOS mgm such that it can work with CTA."
  echo ""
  echo "Usage: $0 [options] -m <mgm-pod-name> -n <namespace>"
  echo ""
  echo "Options:"
  echo "  -h, --help:                     Show help output."
  echo "  -m|--mgm-name <mgm-name>:       The name of the mgm pod to configure."
  echo "  -n|--namespace <namespace>:     The kubernetes namespace the pod lives in."
  echo "  -d|--destination-dir <dir>:     The kubernetes namespace the pod lives in."
  exit 1
}

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    -h | --help) usage ;;
    -m|--mgm-name)
      EOS_MGM_POD="$2"
      shift ;;
    -n|--namespace)
      NAMESPACE="$2"
      shift ;;
    *)
      echo "Unsupported argument: $1"
      usage
      ;;
  esac
  shift
done

if [ -z "$EOS_MGM_POD" ]; then
  echo "Missing mandatory argument: -m | --mgm-name" 1>&2
  exit 1
fi
if [ -z "$NAMESPACE" ]; then
  echo "Missing mandatory argument: -n | --namespace" 1>&2
  exit 1
fi

cd "$(dirname "$0")"
kubectl cp --namespace "${NAMESPACE}" ./configure_eoscta_tape.sh ${EOS_MGM_POD}:/tmp -c eos-mgm
kubectl exec --namespace "${NAMESPACE}" ${EOS_MGM_POD} -c eos-mgm -- /bin/bash -c "chmod +x /tmp/configure_eoscta_tape.sh && /tmp/configure_eoscta_tape.sh"
