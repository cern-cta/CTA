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

CTA_CLI_POD="cta-cli-0"
EOS_MGM_POD="eos-mgm-0"
EOS_MGM_HOST="ctaeos"

executeReclaim() {
    kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin ta reclaim -v $1
    echo "Tape $1 reclaimed"
}

getFirstVidContainingFiles() {
  vidToRepack=$(kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json ta ls --all | jq -r '[.[] | select(.occupancy != "0") | select(.lastFseq != "0") | .vid] | .[0]')
  echo $vidToRepack
}

getNumberOfFilesOnTape() {
  numberOfFiles=$(kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json tf ls --vid $1 | jq -r '. | length')
  echo $numberOfFiles
}

writeTapeSummary(){
  echo "Summary of the content of the tape $1"
  kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json ta ls -v $1 | jq .
}

executeRepack() {
    # This timeout should be enough for all tests to complete
    # If tests are timing out because e.g. a lot of files are being repacked, increase this value
    WAIT_FOR_REPACK_FILE_TIMEOUT=120
    echo
    echo "Changing the tape $1 to FULL status"
    kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin ta ch -v $1 -f true
    echo "Creating the eos directory to put the retrieve files from the repack request"
    kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- rm -rf root://${EOS_MGM_HOST}//eos/ctaeos/repack
    kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos mkdir /eos/ctaeos/repack
    kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos chmod 1777 /eos/ctaeos/repack
    echo "Removing an eventual previous repack request for tape $1"
    kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin re rm -v $1
    echo "Launching the repack request on tape $1"
    kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin re add -v $1 -m -b root://${EOS_MGM_HOST}//eos/ctaeos/repack
    SECONDS_PASSED=0
    while test 0 = $(kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin re ls -v $1 | grep -E "Complete|Failed" | wc -l); do
      echo "Waiting for repack request on tape $1 to be complete: Seconds passed = $SECONDS_PASSED"
      sleep 1
      let SECONDS_PASSED=SECONDS_PASSED+1

      if test ${SECONDS_PASSED} == ${WAIT_FOR_REPACK_FILE_TIMEOUT}; then
        echo "Timed out after ${WAIT_FOR_REPACK_FILE_TIMEOUT} seconds waiting for tape $1 to be repacked"
        exit 1
      fi
    done
    if test 1 = $(kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin re ls -v $1 | grep -E "Failed" | wc -l); then
        echo "Repack failed for tape $1"
        exit 1
    fi
}
