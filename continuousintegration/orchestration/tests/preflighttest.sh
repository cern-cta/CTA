#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2022 CERN
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

usage() { cat <<EOF 1>&2
Usage: $0 -n <namespace>
EOF
exit 1
}

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
shift $((OPTIND-1))

if [ -z "${NAMESPACE}" ]; then
    usage
fi

if [ ! -z "${error}" ]; then
    echo -e "ERROR:\n${error}"
    exit 1
fi

TMPDIR=$(mktemp -d --suffix .testflight)

FLIGHTTEST_RC=0 # flighttest return code
EOSINSTANCE="ctaeos"

echo "Running preflight checks on the following eos version:"
kubectl -n ${NAMESPACE} exec $EOSINSTANCE -- eos version
echo


###
# TPC capabilities
#
# if the installed xrootd version is not the same as the one used to compile eos
# Third Party Copy can be broken
#
# Valid check:
# [root@eosctafst0017 ~]# xrdfs
# root://pps-ngtztag6p5ht-minion-2.cern.ch:1101 query config tpc
# 1
#
# invalid check:
# [root@ctaeos /]# xrdfs root://ctaeos.toto.svc.cluster.local:1095 query config tpc
# tpc
# EOSINSTANCE="cta-mgm-0"

FLIGHTTEST_TPC_RC=0
TEST_LOG="${TMPDIR}/tpc_cap.log"
echo "Checking xrootd TPC capabilities on FSTs:"
kubectl -n ${NAMESPACE} exec $EOSINSTANCE -- eos node ls -m | grep status=online | sed -e 's/.*hostport=//;s/ .*//' | xargs -ifoo bash -o pipefail -c "kubectl -n ${NAMESPACE} exec $EOSINSTANCE -- xrdfs root://foo query config tpc | grep -q 1 && echo foo: OK|| echo foo: KO" | tee ${TEST_LOG}

if $(cat ${TEST_LOG} | grep -q KO); then
  echo "TPC capabilities: FAILED"
  ((FLIGHTTEST_TPC_RC+=1))
else
  echo "TPC capabilities: SUCCESS"
fi
echo


###
# xrootd API FTS compliance
#
# see CTA#1256
# write 3 files and xrdfs query them in reverse order with duplicates
# xrdfs query prepare 3 2 1 3 must answer 3 2 1 3

FLIGHTTEST_XROOTD_API_RC=0
TESTDIR="/eos/ctaeos/tmp"
FILES_JSON=$(for path in 3 2 1 3; do echo "{\"path\": \"${TESTDIR}/${path}\"}"; done | jq --slurp . )
echo "Checking xrootd API FTS compliance"
for TESTFILE in $(echo $FILES_JSON | jq -r '.[].path' | sort -u); do
  kubectl -n ${NAMESPACE} exec $EOSINSTANCE -- eos touch ${TESTFILE}
done
echo ${FILES_JSON} > ${TMPDIR}/xrootd_API_src.json
kubectl -n ${NAMESPACE} exec $EOSINSTANCE -- xrdfs root://localhost query prepare 0 $(echo $FILES_JSON | jq -r '. | map(.path) | join(" ")') > ${TMPDIR}/xrootd_API_query_prepare_result.json

INPUT_FILE_LIST="$(cat ${TMPDIR}/xrootd_API_src.json | jq -r '. | map(.path) | join(" ")')"
OUTPUT_FILE_LIST="$(cat ${TMPDIR}/xrootd_API_query_prepare_result.json | jq -r '.responses| map(.path) | join(" ")')"

if [ "-${INPUT_FILE_LIST}-" == "-${OUTPUT_FILE_LIST}-" ]; then
  echo "xrootd_API capabilities: SUCCESS"
else
  echo "xrootd_API capabilities: FAILED"
  ((FLIGHTTEST_XROOTD_API_RC+=1))
fi

echo "xrootd_API summary"
echo "# -INPUT-:"
echo "-${INPUT_FILE_LIST}-"
echo "# -OUTPUT-:"
echo "-${OUTPUT_FILE_LIST}-"


###
#
# End of preflight test

FLIGHTTEST_RC=$((${FLIGHTTEST_TPC_RC}+${FLIGHTTEST_XROOTD_API_RC}))
echo
if [ ${FLIGHTTEST_RC} -eq 0 ]; then
  echo "PREFLIGHT test: SUCCESS"
else
  echo "PREFLIGHT test: FAILED"
fi

rm -fr ${TMPDIR}
exit ${FLIGHTTEST_RC}
