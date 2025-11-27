#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2024 CERN
# @copyright    Copyright © 2024 DESY
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

usage() {
  cat <<EOF 1>&2
Usage: $0 -n <namespace>
EOF
  exit 1
}

NAMESPACE=""

while getopts "n:" o; do
  case "${o}" in
  n)
    NAMESPACE=${OPTARG}
    ;;
  *)
    usage
    ;;
  esac
done
shift $((OPTIND - 1))

if [[ -z "${NAMESPACE}" ]]; then
  usage
fi

./prepare_tests_dcache.sh -n ${NAMESPACE}

CLIENT_POD="cta-client-0"

kubectl -n "${NAMESPACE}" cp . ${CLIENT_POD}:/root/ -c client || exit 1
kubectl -n "${NAMESPACE}" exec ${CLIENT_POD} -- bash +x /root/client_dcache.sh  || exit 1
