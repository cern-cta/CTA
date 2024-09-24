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
tape_server='tpsrv01'

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

helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo add dcache https://gitlab.desy.de/api/v4/projects/7648/packages/helm/test
helm repo update
helm install -n ${NAMESPACE} --replace --wait --timeout 10m0s --set auth.username=dcache --set auth.password=let-me-in --set auth.database=chimera chimera bitnami/postgresql --version=12.12.10
helm install -n ${NAMESPACE} --replace --wait --timeout 10m0s cells bitnami/zookeeper
helm install -n ${NAMESPACE} --replace --wait --timeout 10m0s --set externalZookeeper.servers=cells-zookeeper --set kraft.enabled=false billing bitnami/kafka --version 23.0.7
helm install -n ${NAMESPACE} --debug --replace --wait --timeout 10m0s --set image.tag=9.2.22 --set dcache.hsm.enabled=true store dcache/dcache
echo
echo "DEBUG INFO... statefulsets"
kubectl -n "${NAMESPACE}" get statefulsets
echo "DEBUG INFO... descibe statefulset"
kubectl -n "${NAMESPACE}" describe statefulset
echo "DEBUG INFO... descibe pod"
kubectl -n "${NAMESPACE}" describe pod store-dcache-door


exit 0
