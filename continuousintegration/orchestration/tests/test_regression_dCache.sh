#!/bin/bash

# SPDX-FileCopyrightText: 2024 CERN
# SPDX-FileCopyrightText: 2024 DESY
# SPDX-License-Identifier: GPL-3.0-or-later

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

if [ -z "${NAMESPACE}" ]; then
  usage
fi

./prepare_tests_dcache.sh -n ${NAMESPACE}

CLIENT_POD="cta-client-0"

kubectl -n "${NAMESPACE}" cp . ${CLIENT_POD}:/root/ -c client || exit 1
kubectl -n "${NAMESPACE}" exec ${CLIENT_POD} -- bash +x /root/client_dcache.sh  || exit 1
