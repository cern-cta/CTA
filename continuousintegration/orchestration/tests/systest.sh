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


# eos instance identified by SSS username
EOSINSTANCE=ctaeos

tempdir=$(mktemp -d) # temporary directory for system test related config
echo -n "Reading library configuration from tpsrv01"
SECONDS_PASSED=0
while test 0 = $(kubectl --namespace ${NAMESPACE} exec tpsrv01 -c taped -- cat /tmp/library-rc.sh | sed -e 's/^export//' | tee ${tempdir}/library-rc.sh | wc -l); do
  sleep 1
  echo -n .
  let SECONDS_PASSED=SECONDS_PASSED+1

  if test ${SECONDS_PASSED} == 30; then
    echo "FAILED"
    echo "Timed out after ${SECONDS_PASSED} seconds waiting for file to be archived to tape"
    exit 1
  fi
done
echo "OK"

echo "Using this configuration for library:"
cat ${tempdir}/library-rc.sh
. ${tempdir}/library-rc.sh

#clean the  library
#  echo "Clean the library /dev/${LIBRARYDEVICE} if needed"
#    mtx -f /dev/${LIBRARYDEVICE} status | sed -e "s/:/ /g"| grep "Full" | awk '{if ($1=="Data" ) { rewind="mt -f /dev/${DRIVEDEVICES["$4"]} rewind"; print rewind; print "Rewind drive "$4>"/dev/stderr"; unload="mtx -f /dev/${LIBRARYDEVICE} unload "$8" "$4; print unload; print "Unloading to storage slot "$8" from data slot "$4"" >"/dev/stderr";}}' |  source /dev/stdin

ctacliIP=`kubectl --namespace ${NAMESPACE} describe pod ctacli | grep IP | sed -E 's/IP:[[:space:]]+//'`

echo "Preparing CTA for tests"
  kubectl --namespace ${NAMESPACE} exec ctafrontend -- cta-catalogue-admin-user-create /etc/cta/cta-catalogue.conf --username admin1 -m "docker cli"
  kubectl --namespace ${NAMESPACE} exec ctacli -- cta logicallibrary add \
     --name ${LIBRARYNAME}                                            \
     --comment "ctasystest"
  kubectl --namespace ${NAMESPACE} exec ctacli -- cta tapepool add       \
    --name ctasystest                                                 \
    --partialtapesnumber 5                                            \
    --encrypted false                                                 \
    --comment "ctasystest"
  # add all tapes
  for ((i=0; i<${#TAPES[@]}; i++)); do
    VID=${TAPES[${i}]}
    kubectl --namespace ${NAMESPACE} exec ctacli -- cta tape add         \
      --logicallibrary ${LIBRARYNAME}                                 \
      --tapepool ctasystest                                           \
      --capacity 1000000000                                           \
      --comment "ctasystest"                                          \
      --vid ${VID}                                                    \
      --full false                                                    \
      --comment "ctasystest"
  done
  kubectl --namespace ${NAMESPACE} exec ctacli -- cta storageclass add   \
    --instance ${EOSINSTANCE}                                            \
    --name ctaStorageClass                                               \
    --numberofcopies 1                                                   \
    --comment "ctasystest"
  kubectl --namespace ${NAMESPACE} exec ctacli -- cta archiveroute add   \
    --instance ${EOSINSTANCE}                                         \
    --storageclass ctaStorageClass                                    \
    --copynb 1                                                        \
    --tapepool ctasystest                                             \
    --comment "ctasystest"
  kubectl --namespace ${NAMESPACE} exec ctacli -- cta mountpolicy add    \
    --name ctasystest                                                 \
    --archivepriority 1                                               \
    --minarchiverequestage 1                                          \
    --retrievepriority 1                                              \
    --minretrieverequestage 1                                         \
    --comment "ctasystest"
  kubectl --namespace ${NAMESPACE} exec ctacli -- cta requestermountrule add \
     --instance ${EOSINSTANCE}                                        \
     --name adm                                                       \
     --mountpolicy ctasystest --comment "ctasystest"
###
# This rule exists to allow users from eosusers group to migrate files to tapes
  kubectl --namespace ${NAMESPACE} exec ctacli -- cta groupmountrule add \
     --instance ${EOSINSTANCE}                                        \
     --name eosusers                                                  \
     --mountpolicy ctasystest --comment "ctasystest"
###
# This rule exists to allow users from powerusers group to recall files from tapes
  kubectl --namespace ${NAMESPACE} exec ctacli -- cta groupmountrule add \
     --instance ${EOSINSTANCE}                                        \
     --name powerusers                                                  \
     --mountpolicy ctasystest --comment "ctasystest"

  kubectl --namespace ${NAMESPACE} exec ctacli --  cta drive up ${DRIVENAMES[${driveslot}]}

# testing
  echo "EOS server version is used:"
    kubectl --namespace ${NAMESPACE} exec ctaeos -- rpm -qa|grep eos-server
  TEST_FILE_NAME=`uuidgen`
  echo "xrdcp /etc/group root://localhost//eos/ctaeos/cta/${TEST_FILE_NAME}"
    kubectl --namespace ${NAMESPACE} exec ctaeos -- xrdcp /etc/group root://localhost//eos/ctaeos/cta/${TEST_FILE_NAME}

  SECONDS_PASSED=0
  WAIT_FOR_ARCHIVED_FILE_TIMEOUT=90
  while test 0 = `kubectl --namespace ${NAMESPACE} exec ctaeos -- eos info /eos/ctaeos/cta/${TEST_FILE_NAME} | awk '{print $4;}' | grep tape | wc -l`; do
    echo "Waiting for file to be archived to tape: Seconds passed = ${SECONDS_PASSED}"
    sleep 1
    let SECONDS_PASSED=SECONDS_PASSED+1

    if test ${SECONDS_PASSED} == ${WAIT_FOR_ARCHIVED_FILE_TIMEOUT}; then
      echo "Timed out after ${WAIT_FOR_ARCHIVED_FILE_TIMEOUT} seconds waiting for file to be archived to tape"
      exit 1
    fi
  done
  echo
  echo "FILE ARCHIVED TO TAPE"
  echo
  kubectl --namespace ${NAMESPACE} exec ctaeos -- eos info /eos/ctaeos/cta/${TEST_FILE_NAME}
  echo
  echo "Information about the testing file:"
  echo "********"
    kubectl --namespace ${NAMESPACE} exec ctaeos -- eos attr ls /eos/ctaeos/cta/${TEST_FILE_NAME}
    kubectl --namespace ${NAMESPACE} exec ctaeos -- eos info /eos/ctaeos/cta/${TEST_FILE_NAME}
  echo
  echo "Removing disk replica"
    kubectl --namespace ${NAMESPACE} exec ctaeos -- eos -r 0 0 file drop /eos/ctaeos/cta/${TEST_FILE_NAME} 1
  echo
  echo "Information about the testing file without disk replica"
    kubectl --namespace ${NAMESPACE} exec ctaeos -- eos info /eos/ctaeos/cta/${TEST_FILE_NAME}
  echo
  echo "trigger EOS retrieve workflow"
  echo "xrdfs localhost prepare -s /eos/ctaeos/cta/${TEST_FILE_NAME}"
    kubectl --namespace ${NAMESPACE} exec ctaeos -- xrdfs localhost prepare -s /eos/ctaeos/cta/${TEST_FILE_NAME}

  # Wait for the copy to appear on disk
  SECONDS_PASSED=0
  WAIT_FOR_RETRIEVED_FILE_TIMEOUT=90
  while test 0 = `kubectl --namespace ${NAMESPACE} exec ctaeos -- eos info /eos/ctaeos/cta/${TEST_FILE_NAME} | awk '{print $4;}' | grep -F "default.0" | wc -l`; do
    echo "Waiting for file to be retrieved from tape: Seconds passed = ${SECONDS_PASSED}"
    sleep 1
    let SECONDS_PASSED=SECONDS_PASSED+1

    if test ${SECONDS_PASSED} == ${WAIT_FOR_RETRIEVED_FILE_TIMEOUT}; then
      echo "Timed out after ${WAIT_FOR_RETRIEVED_FILE_TIMEOUT} seconds waiting for file to be retrieved from tape"
      exit 1
    fi
  done
  echo
  echo "FILE RETRIEVED FROM DISK"
  echo
  echo "Information about the testing file:"
  echo "********"
    kubectl --namespace ${NAMESPACE} exec ctaeos -- eos attr ls /eos/ctaeos/cta/${TEST_FILE_NAME}
    kubectl --namespace ${NAMESPACE} exec ctaeos -- eos info /eos/ctaeos/cta/${TEST_FILE_NAME}

# results
  echo
  msgNum=`kubectl --namespace $NAMESPACE logs tpsrv01 -c taped | grep "\"File suc" | grep ${TEST_FILE_NAME} | tail -n 4|wc -l`
  if [ $msgNum == "4" ]; then
    echo "OK: all tests passed"
      rc=0
  else
    echo "FAIL: tests failed"
      rc=1
  fi


rm -fr ${tempdir}

exit $rc
