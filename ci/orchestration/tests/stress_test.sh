#!/bin/bash

# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later


# simple stress test: archive some files and then retrieve these

usage() { cat <<EOF 1>&2
Usage: $0 -n <namespace> [-Q]

Use -Q to enable pre-queueing
EOF
exit 1
}

get_pods_by_type() {
  local type="$1"
  local namespace="$2"
  kubectl get pod -l app.kubernetes.io/component=$type -n $namespace --no-headers -o custom-columns=":metadata.name"
}

PREQUEUE=0

while getopts "n:Q" o; do
    case "${o}" in
        n)
            NAMESPACE=${OPTARG}
            ;;
        Q)
            PREQUEUE=1
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

if [[ -z "${NAMESPACE}" ]]; then
    usage
fi

CLIENT_POD="cta-client-0"
CTA_CLI_POD="cta-cli-0"
CTA_FRONTEND_POD=$(get_pods_by_type frontend $NAMESPACE | head -1)
EOS_MGM_POD="eos-mgm-0"

NB_FILES=20000
NB_DIRS=250
FILE_SIZE_KB=1
NB_PROCS=30
NB_DRIVES=100

FST=$(kubectl -n ${NAMESPACE} get pods | grep "eos-fst" | awk '{ print $1 }' | head)
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -lc '
  set -euo pipefail

  echo "=== Installing Python 3 ==="
  if ! command -v python3 >/dev/null 2>&1; then
    dnf install -y python3
  fi

  echo "=== Installing XRootD client libraries ==="
  dnf install -y xrootd-client python3-xrootd

  echo "=== Verifying installation ==="
  python3 --version
  python3 - <<EOF
from XRootD import client
print("XRootD Python bindings OK")
EOF

  echo "=== DONE ==="
'

kubectl -n ${NAMESPACE} cp client_helper.sh ${CLIENT_POD}:/root/client_helper.sh -c client
kubectl -n ${NAMESPACE} cp xrdcp_persistent_client_stress.py ${CLIENT_POD}:/root/xrdcp_persistent_client_stress.py -c client
cp xrdcp_persistent_client_stress.py xrdcp_persistent_client_stress_fst.py
sed -i 's/eos.keytab/fstuser.keytab/' xrdcp_persistent_client_stress_fst.py
kubectl -n ${NAMESPACE} cp xrdcp_persistent_client_stress_fst.py ${FST}:/root/xrdcp_stress_archive.py -c eos-fst
#mkdir /tmp/ctaadmin2
#mkdir /tmp/eosadmin1
#mkdir /tmp/poweruser1
#mkdir -p /tmp/ctaadmin2 /tmp/eosadmin1 /tmp/poweruser1
#kubectl -n ${NAMESPACE} cp ${CLIENT_POD}:/tmp/ctaadmin2/krb5cc_0 /tmp/ctaadmin2/krb5cc_0 -c client
#kubectl -n ${NAMESPACE} cp ${CLIENT_POD}:/tmp/eosadmin1/krb5cc_0 /tmp/eosadmin1/krb5cc_0 -c client
#kubectl -n ${NAMESPACE} cp ${CLIENT_POD}:/tmp/krb5cc_0 /tmp/krb5cc_0 -c client
#kubectl -n ${NAMESPACE} cp ${CLIENT_POD}:/tmp/poweruser1/krb5cc_0 /tmp/poweruser1/krb5cc_0 -c client
#kubectl exec ${FST} -c eos-fst -n ${NAMESPACE} -- mkdir -p /tmp/ctaadmin2 /tmp/eosadmin1 /tmp/poweruser1
#kubectl exec ${FST} -c eos-fst -n ${NAMESPACE} -- yum install -y python3
#kubectl -n ${NAMESPACE} cp /tmp/ctaadmin2/krb5cc_0 ${FST}:/tmp/ctaadmin2/krb5cc_0 -c eos-fst
#kubectl -n ${NAMESPACE} cp /tmp/eosadmin1/krb5cc_0 ${FST}:/tmp/eosadmin1/krb5cc_0 -c eos-fst
#kubectl -n ${NAMESPACE} cp /tmp/krb5cc_0 ${FST}:/tmp/krb5cc_0 -c eos-fst
#kubectl -n ${NAMESPACE} cp /tmp/poweruser1/krb5cc_0 ${FST}:/tmp/poweruser1/krb5cc_0 -c eos-fst

# Need CTAADMIN_USER krb5
admin_kdestroy &>/dev/null
admin_kinit &>/dev/null

echo " Copying CA certificates to client pod from ${EOS_MGM_POD} pod."
kubectl -n ${NAMESPACE} cp "${EOS_MGM_POD}:etc/grid-security/certificates/" /tmp/certificates/ -c eos-mgm
kubectl -n ${NAMESPACE} cp /tmp/certificates ${CLIENT_POD}:/etc/grid-security/ -c client
kubectl -n ${NAMESPACE} cp /tmp/certificates ${FST}:/etc/grid-security/ -c eos-fst
rm -rf /tmp/certificates

kubectl -n ${NAMESPACE} cp "${EOS_MGM_POD}:etc/eos.keytab" /tmp/eos.keytab -c eos-mgm
sed -i 's/u:daemon g:daemon/u:user1 g:eosusers/' /tmp/eos.keytab
kubectl -n ${NAMESPACE} cp /tmp/eos.keytab "${CLIENT_POD}:etc/eos.keytab" -c client
kubectl -n ${NAMESPACE} cp /tmp/eos.keytab "${FST}:etc/fstuser.keytab" -c eos-fst
MY_CLIENT_SSS_KEY=$(kubectl exec ${EOS_MGM_POD} -n dev -c eos-mgm -- bash -c "cat /etc/eos.keytab")
kubectl exec cta-client-0 -n ${NAMESPACE}  -c client -- bash -c "chmod 400 /etc/eos.keytab"
kubectl exec ${FST} -n ${NAMESPACE} -c eos-fst -- bash -c "chmod 400 /etc/fstuser.keytab"

kubectl exec ${EOS_MGM_POD} -n ${NAMESPACE} -c eos-mgm -- bash -c "chmod 770 /etc/eos.keytab"
kubectl exec ${EOS_MGM_POD} -n ${NAMESPACE} -c eos-mgm -- bash -c "echo ${MY_CLIENT_SSS_KEY} >> /etc/eos.keytab"
kubectl exec ${EOS_MGM_POD} -n ${NAMESPACE} -c eos-mgm -- bash -c "chmod 400 /etc/eos.keytab"

echo "Putting all tape drives down - to queue the files first since the processing is faster than the queueing capabilities of EOS, we hold it half-way and only then put drives up in the client_stress_ar.sh script"
kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin dr down '.*' --reason "pre-queue jobs"

#echo "Putting all tape drives up"
#kubectl -n ${NAMESPACE} exec ctacli -- cta-admin dr up '.*'

#kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin mp ch --name ctasystest --minarchiverequestage 100 --minretrieverequestage 100 --maxdrivesallowed ${NB_DRIVES} --comment "ctasystest for stress tests"

kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin mp ch --name ctasystest --minarchiverequestage 100 --minretrieverequestage 100 --comment "ctasystest for stress tests"

kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin vo ch --vo vo --writemaxdrives ${NB_DRIVES} --readmaxdrives ${NB_DRIVES}

kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -it -c cta-cli -- cta-admin mp ls

kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -it -c cta-cli -- cta-admin dr ls

kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos fs config 1 scaninterval=0
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos fs config 2 scaninterval=0
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos fs config 3 scaninterval=0
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos fs config 4 scaninterval=0
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos fs config 5 scaninterval=0
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos fs config 6 scaninterval=0
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos fs config 7 scaninterval=0
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos fs config 8 scaninterval=0
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos fs config 9 scaninterval=0
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos fs config 10 scaninterval=0
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos fs config 11 scaninterval=0
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos fs config 12 scaninterval=0
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos fs config 13 scaninterval=0

# install eos-debuginfo (600MB -> only for stress tests)
# NEEDED because eos does not leave the coredump after a crash
# Commented out for now as the EOS images do not provide debuginfo
# kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- dnf install -y eos-debuginfo

kubectl -n ${NAMESPACE} exec ${CTA_FRONTEND_POD} -c cta-frontend -- dnf install -y xrootd-debuginfo

echo
echo "Launching client_stress_ar.sh on ${CLIENT_POD} pod"
echo " Archiving ${NB_FILES} files of ${FILE_SIZE_KB}kB each"
echo " Archiving files: xrdcp as user1"
echo " Retrieving them as poweruser1"
kubectl -n ${NAMESPACE} cp client_stress_ar.sh ${CLIENT_POD}:/root/client_stress_ar.sh -c client
kubectl -n ${NAMESPACE} cp client_stress_ar.sh ${FST}:/root/client_stress_ar.sh -c eos-fst

EXTRA_TEST_OPTIONS=""
if [[ $PREQUEUE == 1 ]]; then
  EXTRA_TEST_OPTIONS+=" -Q"
fi
kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin dr down '.*' --reason "pre-queue jobs"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -lc 'command -v xrdfs && xrdfs -h | head -n 1'

kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -lc '
  set -e
  export XrdSecsssKT=/etc/eos.keytab
  export XRD_LOGLEVEL=Error

  export NB_FILES=5000000
  export NB_PROCS=40
  export NB_DIRS=500
  export NB_FILES_TO_PUT_DRIVES_UP=4000000
  export CHECK_EVERY_SEC=900
  export STALL_SEC=900

  export EOS_MGM_HOST=ctaeos
  export EOS_DIR=/eos/ctaeos/cta
  export KRB5CC_CTAADMIN2=/tmp/ctaadmin2/krb5cc_0
  python3 -u /root/xrdcp_persistent_client_stress.py
'
exit 0
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash /root/client_stress_ar.sh -N ${NB_DIRS} -n ${NB_FILES} -s ${FILE_SIZE_KB} -p ${NB_PROCS} -e ctaeos -d /eos/ctaeos/cta ${EXTRA_TEST_OPTIONS} || exit 1
kubectl -n ${NAMESPACE} exec ${FST} -c eos-fst -- bash /root/client_stress_ar.sh -F -N ${NB_DIRS} -n ${NB_FILES} -s ${FILE_SIZE_KB} -p ${NB_PROCS} -e ctaeos -d /eos/ctaeos/cta ${EXTRA_TEST_OPTIONS} || exit 1
## Do not remove as listing af is not coming back???
#kubectl -n ${NAMESPACE} exec client -- bash /root/client_stress_ar.sh -A -N ${NB_DIRS} -n ${NB_FILES} -s ${FILE_SIZE_KB} -p ${NB_PROCS} -e ctaeos -d /eos/ctaeos/preprod -v || exit 1

#kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

# REPACK TEST for postgres scheduler only
source ./repack_helper.sh
kubectl -n ${NAMESPACE} cp repack_systemtest.sh ${CLIENT_POD}:/root/repack_systemtest.sh -c client
REPACK_BUFFER_URL=/eos/ctaeos/repack
BASE_REPORT_DIRECTORY=/var/log

modifyTapeState() {
  reason="${3:-Testing}"
  kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin tape ch --state $2 --reason "$reason" --vid $1
}

modifyTapeStateAndWait() {
  WAIT_FOR_EMPTY_QUEUE_TIMEOUT=60
  SECONDS_PASSED=0
  modifyTapeState $1 $2 $3
  echo "Waiting for tape $1 to complete transitioning to $2"
  while test 0 == `kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json tape ls --state $2 --vid $1  | jq -r ". [] | select(.vid == \"$1\")" | wc -l`; do
    sleep 1
    printf "."
    let SECONDS_PASSED=SECONDS_PASSED+1
    if test ${SECONDS_PASSED} == ${WAIT_FOR_EMPTY_QUEUE_TIMEOUT}; then
      echo "Timed out after ${WAIT_FOR_EMPTY_QUEUE_TIMEOUT} seconds waiting for tape $1 to transition to state $2. Test failed."
      exit 1
    fi
  done
}

repackMoveAndAddCopies() {
  echo
  echo "*******************************************************"
  echo " Testing Repack \"Move and Add copies\" workflow       "
  echo "*******************************************************"

  defaultTapepool="ctasystest"
  tapepoolDestination1_default="systest2_default"
  tapepoolDestination2_default="systest3_default"
  tapepoolDestination2_repack="systest3_repack"

  echo "Creating 2 destination tapepools : $tapepoolDestination1_default and $tapepoolDestination2_default"
  kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin tapepool add --name $tapepoolDestination1_default --vo vo --partialtapesnumber 2 --comment "$tapepoolDestination1_default tapepool"
  kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin tapepool add --name $tapepoolDestination2_default --vo vo --partialtapesnumber 2 --comment "$tapepoolDestination2_default tapepool"
  echo "OK"

  echo "Creating 1 destination tapepool for repack : $tapepoolDestination2_repack (will override $tapepoolDestination2_default)"
  kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin tapepool add --name $tapepoolDestination2_repack --vo vo --partialtapesnumber 2 --comment "$tapepoolDestination2_repack tapepool"
  echo "OK"

  echo "Creating archive routes for adding two copies of the file"
  kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin archiveroute add --storageclass ctaStorageClass --copynb 2 --tapepool $tapepoolDestination1_default --comment "ArchiveRoute2_default"
  kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin archiveroute add --storageclass ctaStorageClass --copynb 3 --archiveroutetype DEFAULT --tapepool $tapepoolDestination2_default --comment "ArchiveRoute3_default"
  kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin archiveroute add --storageclass ctaStorageClass --copynb 3 --archiveroutetype REPACK --tapepool $tapepoolDestination2_repack --comment "ArchiveRoute3_repack"
  echo "OK"

  echo "Will change the tapepool of the tapes"

  allVID=$(kubectl -n ${NAMESPACE}  exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json tape ls --all | jq -r ". [] | .vid")
  allVIDTable=($allVID)

  nbVid=${#allVIDTable[@]}

  allTapepool=$(kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json tapepool ls | jq -r ". [] .name")
  allTapepoolTable=($allTapepool)

  nbTapepool=${#allTapepoolTable[@]}
  nbTapePerTapepool=$(($nbVid / $nbTapepool))

  countChanging=0
  tapepoolIndice=1 #We only change the vid of the remaining other tapes
  if [[ $nbTapepool -eq 0 ]]; then
    echo "No tapepools found for repack — aborting."
    exit 1
  fi
  for ((i=$(($nbTapePerTapepool+$(($nbVid%$nbTapepool)))); i<$nbVid; i++));
  do
    echo "kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin tape ch --vid ${allVIDTable[$i]} --tapepool ${allTapepoolTable[$tapepoolIndice]}"
    kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin tape ch --vid ${allVIDTable[$i]} --tapepool ${allTapepoolTable[$tapepoolIndice]}
    countChanging=$((countChanging + 1))
    if [[ $countChanging -ne 0 && $((countChanging % nbTapePerTapepool)) -eq 0 ]]; then
      tapepoolIndice=$((tapepoolIndice + 1))
    fi
  done

  echo "OK"

  storageClassName=$(kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json storageclass ls | jq -r ". [0] | .name")

  echo "Changing the storage class $storageClassName nb copies"
  kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin storageclass ch --name $storageClassName --numberofcopies 3
  echo "OK"

  echo "Putting all drives up"
  kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin dr up '.*'
  echo "OK"

  VID_LIST=$(getVidsContainingFiles 5)
  for VID_TO_REPACK in ${VID_LIST}; do
      echo "Creating the repack buffer directory for VID (${REPACK_BUFFER_URL}/${VID_TO_REPACK})"
      kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos mkdir ${REPACK_BUFFER_URL}/${VID_TO_REPACK}
      kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos chmod 1777 ${REPACK_BUFFER_URL}/${VID_TO_REPACK}
      kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos ls -la /eos/ctaeos/repack/${VID_TO_REPACK}

      echo "Marking the tape ${VID_TO_REPACK} as REPACKING"
      modifyTapeStateAndWait ${VID_TO_REPACK} REPACKING "MarkingTapeRepacking"
      echo "Launching the repack \"Move and add copies\" test on VID ${VID_TO_REPACK}"
      kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash /root/repack_systemtest.sh -v ${VID_TO_REPACK} -b ${REPACK_BUFFER_URL} -t 300 -r ${BASE_REPORT_DIRECTORY}/RepackMoveAndAddCopies -n repack_ctasystest  || exit 1

      repackLsResult=$(kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json repack ls --vid ${VID_TO_REPACK} | jq ". [0]")
      totalFilesToRetrieve=$(echo $repackLsResult | jq -r ".totalFilesToRetrieve")
      totalFilesToArchive=$(echo $repackLsResult | jq -r ".totalFilesToArchive")
      retrievedFiles=$(echo $repackLsResult | jq -r ".retrievedFiles")
      archivedFiles=$(echo $repackLsResult | jq -r ".archivedFiles")

      if [[ $retrievedFiles != $totalFilesToRetrieve ]]
      then
        echo "RetrievedFiles ($retrievedFiles) != totalFilesToRetrieve ($totalFilesToRetrieve), test FAILED"
        exit 1
      else
        echo "RetrievedFiles ($retrievedFiles) = totalFilesToRetrieve ($totalFilesToRetrieve), OK"
      fi

      if [[ $archivedFiles != $totalFilesToArchive ]]
      then
        echo "ArchivedFiles ($archivedFiles) != totalFilesToArchive ($totalFilesToArchive), test FAILED"
        exit 1
      else
         echo "ArchivedFiles ($archivedFiles) == totalFilesToArchive ($totalFilesToArchive), OK"
      fi
      # Check that 2 copies were written to default tapepool (archive route 1 and 2) and 1 copy to repack tapepool (archive route 3)
      TAPEPOOL_LIST=$(kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -- cta-admin --json repack ls --vid ${VID_TO_REPACK} | jq ".[] | .destinationInfos[] | .vid" | xargs -I{} kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json tape ls --vid {} | jq -r '.[] .tapepool')

      if [[ $TAPEPOOL_LIST != *"$defaultTapepool"* ]]; then
        echo "Did not find $defaultTapepool in repack archive destination pools. Archive route failed."
        exit 1
      else
        echo "Found $defaultTapepool in repack archive destination pools."
      fi
      if [[ $TAPEPOOL_LIST != *"$tapepoolDestination1_default"* ]]; then
        echo "Did not find $tapepoolDestination1_default in repack archive destination pools. Archive route failed."
        exit 1
      else
        echo "Found $tapepoolDestination1_default in repack archive destination pools."
      fi
      if [[ $TAPEPOOL_LIST != *"$tapepoolDestination2_repack"* ]]; then
        echo "Did not find $tapepoolDestination2_repack in repack archive destination pools. Archive route failed."
        exit 1
      else
        echo "Found $tapepoolDestination2_repack in repack archive destination pools."
      fi

      kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos ls -la /eos/ctaeos/repack/${VID_TO_REPACK}
      echo "----"
      removeRepackRequest ${VID_TO_REPACK}
      echo "Setting the tape ${VID_TO_REPACK} back to ACTIVE"
      modifyTapeState ${VID_TO_REPACK} ACTIVE
      echo "Reclaiming tape ${VID_TO_REPACK}"
      kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin tape reclaim --vid ${VID_TO_REPACK}
      kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos ls -la /eos/ctaeos/repack/${VID_TO_REPACK}
      echo "Testing Repack \"Move and Add copies\" workflow TEST OK for ${VID_TO_REPACK}"
  done

  echo
  echo "***************************************************************"
  echo " Testing Repack \"Move and Add copies\" workflow TEST OK       "
  echo "***************************************************************"
}

if [[ $PREQUEUE == 1 ]]; then
  echo "Creating the repack buffer URL directory (${REPACK_BUFFER_URL})"
  kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos mkdir ${REPACK_BUFFER_URL}
  kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos chmod 1777 ${REPACK_BUFFER_URL}
  repackMoveAndAddCopies
fi


exit 0
